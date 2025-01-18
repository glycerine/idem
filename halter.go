package idem

import (
	"fmt"
	"sync"
	"time"
)

// IdemCloseChan can have Close() called on it
// multiple times, and it will only close
// Chan once.
type IdemCloseChan struct {
	Chan   chan struct{}
	closed bool
	mut    sync.Mutex

	children []*IdemCloseChan
}

// Reinit re-allocates the Chan, assinging
// a new channel and reseting the state
// as if brand new.
//
// This breaks the assumptions of the
// recursive child close that ReqStop makes,
// so avoid it if you depend on that.
func (c *IdemCloseChan) Reinit() {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.Chan = make(chan struct{})
	c.closed = false
}

// NewIdemCloseChan makes a new IdemCloseChan.
func NewIdemCloseChan() *IdemCloseChan {
	return &IdemCloseChan{
		Chan: make(chan struct{}),
	}
}

var ErrAlreadyClosed = fmt.Errorf("Chan already closed")

// Close returns ErrAlreadyClosed if it has been
// called before. It never closes IdemClose.Chan more
// than once, so it is safe to ignore the returned
// error value. Close() is safe for concurrent access by multiple
// goroutines. Close returns nil on the initial call, and
// ErrAlreadyClosed on any subsequent call.
//
// When we actually close the underlying c.Chan,
// we also call Close on any children, while holding
// our mutex. This does not happen if the underlying
// channel is already closed, so the recursion stops at
// the depth of the first already-closed node in the tree.
func (c *IdemCloseChan) Close() error {
	c.mut.Lock()
	defer c.mut.Unlock()
	if !c.closed {
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.Close()
		}
		return nil
	}
	return ErrAlreadyClosed
}

// IsClosed tells you if Chan is already closed or not.
func (c *IdemCloseChan) IsClosed() bool {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.closed
}

// Halter helps shutdown a goroutine
type Halter struct {
	// The owning goutine should call Done.Close() as its last
	// action once it has received the ReqStop() signal.
	Done *IdemCloseChan

	// Other goroutines call ReqStop.Close() in order
	// to request that the owning goroutine stop immediately.
	// The owning goroutine should select on ReqStop.Chan
	// in order to recognize shutdown requests.
	ReqStop *IdemCloseChan

	// cmut protects children
	cmut     sync.Mutex
	children []*Halter
}

func NewHalter() *Halter {
	return &Halter{
		Done:    NewIdemCloseChan(),
		ReqStop: NewIdemCloseChan(),
	}
}

// RequestStop closes the h.ReqStop channel
// if it has not already done so. Safe for
// multiple goroutine access.
func (h *Halter) RequestStop() {
	h.ReqStop.Close()
}

// MarkDone closes the h.Done channel
// if it has not already done so. Safe for
// multiple goroutine access.
func (h *Halter) MarkDone() {
	h.Done.Close()
}

// IsStopRequested returns true iff h.ReqStop has been Closed().
func (h *Halter) IsStopRequested() bool {
	return h.ReqStop.IsClosed()
}

// IsDone returns true iff h.Done has been Closed().
func (h *Halter) IsDone() bool {
	return h.Done.IsClosed()
}

// AddChild adds a child IdemCloseChan that will
// be closed when its parent is Close()-ed.
func (c *IdemCloseChan) AddChild(child *IdemCloseChan) {
	c.mut.Lock()
	c.children = append(c.children, child)
	c.mut.Unlock()
}

func (c *IdemCloseChan) RemoveChild(child *IdemCloseChan) {
	c.mut.Lock()
	defer c.mut.Unlock()

	for i, ch := range c.children {
		if ch == child {
			c.children = append(c.children[:i], c.children[i+1:]...)
			return
		}
	}
}

// AddChild adds child to h's children; and
// adds child.ReqStop to the children of
// h.ReqStop.
func (h *Halter) AddChild(child *Halter) {
	h.cmut.Lock()
	h.children = append(h.children, child)
	h.ReqStop.AddChild(child.ReqStop)
	h.cmut.Unlock()
}

// RemoveChild reverses AddChild.
func (h *Halter) RemoveChild(child *Halter) {
	h.cmut.Lock()
	defer h.cmut.Unlock()
	for i, ch := range h.children {
		if ch == child {
			h.children = append(h.children[:i], h.children[i+1:]...)
			return
		}
	}
	h.ReqStop.RemoveChild(child.ReqStop)
}

// StopTreeAndWaitTilDone first calls ReqStop.Close()
// on h (which will recursively call ReqStop.Close()
// on all non-closed children in the ReqStop parallel,
// tree, efficiently stopping at the first already-closed node).
// Then it waits up to atMost time
// for all tree members to Close their
// Done.Chan. It may return much more quickly
// than atMost, but will never wait longer.
// An atMost duration <= 0 will wait indefinitely.
func (h *Halter) StopTreeAndWaitTilDone(atMost time.Duration) {

	// since ReqStop keeps a parallel tree, we only
	// need do this on the top level;
	// to efficiently stop traversal at first already
	// closed tree level.
	h.ReqStop.Close()

	h.waitTilDoneOrAtMost(atMost)
}

// WaitTilDoneOrAtMost waits to return until
// all descendents have closed their Done.Chan or atMost
// time has elapsed. If atMost is <= 0, then we
// wait indefinitely for all the Done.Chan.
func (h *Halter) waitTilDoneOrAtMost(atMost time.Duration) {

	// a nil timeout channel will never fire in a select.
	var to <-chan time.Time
	if atMost > 0 {
		to = time.After(atMost)
	}
	h.visit(func(y *Halter) {
		select {
		case <-y.Done.Chan:
		case <-to:
		}
	})
}

// visit calls f on each member of h's Halter tree in
// a pre-order traversal. f(h) is called,
// and then f(d) is called recusively on
// all descendants d of h. Children are
// visited while holding their parent's
// child mutex cmut, but the parent itself is not.
func (h *Halter) visit(f func(y *Halter)) {
	f(h)

	h.cmut.Lock()
	defer h.cmut.Unlock()

	for _, child := range h.children {
		child.visit(f)
	}
}
