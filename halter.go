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

	childmut sync.Mutex // protects children
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

func (h *Halter) AddChild(child *Halter) {
	h.childmut.Lock()
	h.children = append(h.children, child)
	h.childmut.Unlock()
}

func (h *Halter) RemoveChild(child *Halter) {
	h.childmut.Lock()
	defer h.childmut.Unlock()
	for i, ch := range h.children {
		if ch == child {
			h.children = append(h.children[:i], h.children[i+1:]...)
			return
		}
	}
}

// StopTreeAndWaitTilDone first calls ReqStop.Close()
// recursively on all children in the Halter tree.
// Then it waits up to atMost time
// for all tree members to Close their
// Done.Chan. It may return much more quickly
// than atMost, but will never wait longer.
// A <= atMost duration will wait indefinitely.
func (h *Halter) StopTreeAndWaitTilDone(atMost time.Duration) {
	h.StopTree()
	h.waitTilDoneOrAtMost(atMost)
}

// StopTree calls ReqStop.Close() on all
// the Halter in h's tree of descendents.
func (h *Halter) StopTree() {
	h.visit(func(y *Halter) {
		y.ReqStop.Close()
	})
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
// all descendants d of h.
func (h *Halter) visit(f func(y *Halter)) {
	f(h)

	h.childmut.Lock()
	snapshot := append([]*Halter{}, h.children...)
	h.childmut.Unlock()

	for _, child := range snapshot {
		child.visit(f)
	}
}
