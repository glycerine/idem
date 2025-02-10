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

	// not exported b/c it would lead to data races.
	// Use Reason() to safely extract.
	whyClosed error // CloseWithReason() sets this.

	taskCount int
}

// ErrGiveUp is returned by IdemCloseChan.WaitTilDone
// if the giveup channel close was the reason that
// WaitTilDone returned.
var ErrGiveUp = fmt.Errorf("giveup channel was closed.")

// ErrNotClosed is returned by FirstTreeReason if
// it is called on a tree that is not completely closed.
var ErrNotClosed = fmt.Errorf("tree is not closed")

// ErrTasksAllDone is the reason when the task count reached 0.
var ErrTasksAllDone = fmt.Errorf("tasks all done")

// Delete this as it makes it hard to reason
// about the state of the tree.
//
// // Reinit re-allocates the Chan, assinging
// // a new channel and reseting the state
// // as if brand new.
// //
// // This breaks the assumptions of the
// // recursive child close that ReqStop makes,
// // so avoid it if you depend on that.
// //
// // We do not change the state of any children
// // in our tree.
// func (c *IdemCloseChan) Reinit() {
// 	c.mut.Lock()
// 	defer c.mut.Unlock()
// 	c.Chan = make(chan struct{})
// 	c.closed = false
// }

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

// CloseWithReason is a no-op if c is already closed.
// Otherwise, we record the why and close c.
// Use Reason() to get the why back. Like
// Close(), we recursive call CloseWithReason()
// on any children. Naturally decendents
// will only set why if they were still open
// when the recursive CloseWithReason gets to them,
// and if any child is already closed the
// recursion stops.
func (c *IdemCloseChan) CloseWithReason(why error) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	if !c.closed {
		c.whyClosed = why
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.CloseWithReason(why)
		}
		return nil
	}
	return ErrAlreadyClosed
}

// Reason gets the why set with CloseWithReason().
// It also returns the closed state of c.
// If c is still open, why will be nil,
// as why is only set during the initial CloseWithReason
// if c was still open.
// The returned why will also be nil
// if c was closed without a reason, by Close()
// for example.
//
// Callers should be prepared to handle a nil why
// no matter what the state of isClosed.
func (c *IdemCloseChan) Reason() (why error, isClosed bool) {
	c.mut.Lock()
	defer c.mut.Unlock()
	why = c.whyClosed
	isClosed = c.closed
	return
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
// be closed when its parent is Close()-ed. If
// c is already closed, we close child immediately.
func (c *IdemCloseChan) AddChild(child *IdemCloseChan) {
	c.mut.Lock()
	if c.closed {
		child.CloseWithReason(c.whyClosed)
	}
	c.children = append(c.children, child)
	//vv("IdemCloseChan.AddChild() now has %v children", len(c.children))
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
//
// Note that if h.ReqStop is already closed,
// then child.ReqStop will also be
// ClosedWithReason with the same reason
// as h, if any is available.
func (h *Halter) AddChild(child *Halter) {
	h.cmut.Lock()
	h.children = append(h.children, child)
	h.ReqStop.AddChild(child.ReqStop)
	h.cmut.Unlock()
}

// RemoveChild removes child from the set
// of h.children. It does not undo any
// close operation that AddChild did on addition.
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

// StopTreeAndWaitTilDone first calls ReqStop.CloseWithReason(why)
// on h (which will recursively call ReqStop.CloseWithReason(why)
// on all non-closed children in the ReqStop parallel,
// tree, efficiently stopping at the first already-closed node).
// Then it waits up to atMost time
// for all tree members to Close their
// Done.Chan. It may return much more quickly
// than atMost, but will never wait longer.
// An atMost duration <= 0 will wait indefinitely.
func (h *Halter) StopTreeAndWaitTilDone(atMost time.Duration, why error) {

	// since ReqStop keeps a parallel tree, we only
	// need do this on the top level;
	// to efficiently stop traversal at first already
	// closed tree level.
	h.ReqStop.CloseWithReason(why)

	// We have to close our own Done channel.
	// Otherwise, waitTilDoneOrAtMost will block, waiting for it.
	h.Done.CloseWithReason(why)

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

// WaitTilClosed does not return until either
// a) we and all our children have closed our
// channels; or b) the supplied giveup channel
// has been closed.
//
// We recursively call WaitTilClosed on all of
// our children, and then wait for our own close.
//
// Note we cannot use a time.After channel for giveup,
// since that only fires once--we have possibly
// many children to wait for.
// Hence giveup must be _closed_, not just sent on,
// to have them all stop waiting.
//
// There is no protection against racey mutation
// of our child tree once this operation has started.
// The user must guarantee that nobody will
// do AddChild or RemoveChild while we are
// waiting.
//
// If the giveup channel was closed, the returned
// error will be ErrGiveUp. Otherwise it will
// be the first Reason() supplied by any
// CloseWithReason() calls in the tree. The
// first non-nil 'why' error we encounter
// is sticky. If err is returned nil, you
// know that either CloseWithReason was
// not used, or that there were only nil
// reason closes.
func (c *IdemCloseChan) WaitTilClosed(giveup <-chan struct{}) (err error) {
	c.mut.Lock()
	if c.closed {
		err = c.whyClosed
		c.mut.Unlock()
		return
	}
	// avoid data races if our advice above is ignored.
	bairn := append([]*IdemCloseChan{}, c.children...)
	c.mut.Unlock()
	// INVAR: we were open, and c.mut is now not held,
	// so we can be closed.
	for _, child := range bairn {
		err1 := child.WaitTilClosed(giveup)
		// first error is sticky
		if err == nil {
			err = err1
		}
	}
	select {
	case <-c.Chan:
		// first error is still sticky, don't overwrite with c.Reason().
		if err != nil {
			return
		}
		err, _ = c.Reason()
		return
	case <-giveup:
		return ErrGiveUp
	}
}

// WaitTilChildrenClosed is just like WaitTilClosed, but
// does not require (or check) if we ourselves, the root
// of our tree, is closed.
func (c *IdemCloseChan) WaitTilChildrenClosed(giveup <-chan struct{}) (err error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	for _, child := range c.children {
		child.WaitTilClosed(giveup)
	}
	for _, child := range c.children {
		err = child.FirstTreeReason()
		if err != nil {
			return err
		}
	}
	return
}

// FirstTreeReason scans the whole tree root at c
// for the first non-nil close reason, and returns it.
// If the tree is not completely closed at any node,
// it returns ErrNotClosed.
func (c *IdemCloseChan) FirstTreeReason() (err error) {

	why, isClosed := c.Reason()
	if !isClosed {
		return ErrNotClosed
	}
	if why != nil {
		return why
	}

	c.mut.Lock()
	defer c.mut.Unlock()
	for _, child := range c.children {
		err = child.FirstTreeReason()
		if err != nil {
			return err
		}
	}
	return
}

// TaskAdd adds delta to the taskCount. When
// the taskCount reaches zero (or below),
// this channel will be closed.
//
// TaskAdd, TaskDone, TaskWait are like
// sync.WaitGroup but with sane integration
// with other channels; use the giveup channel
// in TaskWait, or just integreate c.Chan
// into your own select loop.
func (c *IdemCloseChan) TaskAdd(delta int) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.taskCount += delta
	if c.taskCount <= 0 {
		//vv("taskCount is now %v, closing ourselves/all children.", c.taskCount)
		// inlined CloseWithReason, since we hold the mut.
		if !c.closed {
			c.whyClosed = ErrTasksAllDone
			close(c.Chan)
			c.closed = true
			for _, child := range c.children {
				child.CloseWithReason(c.whyClosed)
			}
		}
	} else {
		//vv("taskCount is now %v", c.taskCount)
	}
}

// TaskWait waits to return until either c or giveup
// is closed, whever happens first.
//
// TaskAdd, TaskDone, TaskWait are like
// sync.WaitGroup but with sane integration
// with other channels.
//
// You can readily integrate c.Chan into your
// own select statement.
func (c *IdemCloseChan) TaskWait(giveup <-chan struct{}) (err error) {
	select {
	case <-giveup:
		return ErrGiveUp
	case <-c.Chan:
		err, _ = c.Reason()
	}
	return
}

// TaskDone subtracts one from the task count.
func (c *IdemCloseChan) TaskDone() {
	c.TaskAdd(-1)
}
