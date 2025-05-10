package idem

import (
	"fmt"
	//"sync"
	"time"
)

// to avoid locking/deadlock issues, processes
// including this package get exactly one
// process-global single mutex for all Halter
// and IdemCloseChan tree operations.
var globalTreeLock *globalSingleLock

type globalSingleLock struct {
	availCh chan struct{}
}

func init() {
	s := &globalSingleLock{
		availCh: make(chan struct{}, 1),
	}
	s.availCh <- struct{}{}
	globalTreeLock = s
}

func lock(bail chan struct{}) bool {
	select {
	case <-globalTreeLock.availCh:
		return true
	case <-bail:
		return false
	}
}
func unlock(bail chan struct{}) bool {
	select {
	case globalTreeLock.availCh <- struct{}{}:
		return true
	case <-bail:
		return false
	}
}

// IdemCloseChan can have Close() called on it
// multiple times, and it will only close
// Chan once.
type IdemCloseChan struct {
	Chan   chan struct{}
	closed bool

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
	lock(nil) // hung here Test106TaskWait
	defer unlock(nil)
	return c.unlockedClose()
}
func (c *IdemCloseChan) unlockedClose() error {
	if !c.closed {
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.unlockedClose()
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
	lock(nil)
	defer unlock(nil)
	return c.unlockedCloseWithReason(why)
}

func (c *IdemCloseChan) unlockedCloseWithReason(why error) error {
	if !c.closed {
		c.whyClosed = why
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.unlockedCloseWithReason(why)
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
	lock(nil)
	defer unlock(nil)
	return c.unlockedReason()
}

func (c *IdemCloseChan) unlockedReason() (why error, isClosed bool) {
	why = c.whyClosed
	isClosed = c.closed
	return
}

// Reason1 is the same as Reason but without the isClosed.
// This is easier to use in logging.
func (c *IdemCloseChan) Reason1() (why error) {
	lock(nil)
	defer unlock(nil)
	return c.unlockedReason1()
}

func (c *IdemCloseChan) unlockedReason1() (why error) {
	why = c.whyClosed
	return
}

// IsClosed tells you if Chan is already closed or not.
func (c *IdemCloseChan) IsClosed() bool {
	lock(nil)
	defer unlock(nil)
	return c.closed
}

func (c *IdemCloseChan) unlockedIsClosed() bool {
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

	children []*Halter
}

func NewHalter() (h *Halter) {
	h = &Halter{
		Done:    NewIdemCloseChan(),
		ReqStop: NewIdemCloseChan(),
	}
	//vv("NewHalter created h=%p at %v", h, stack())
	return
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
	if child == c {
		panic("cannot add ourselves as a child of ourselves; would deadlock")
	}
	lock(nil)
	defer unlock(nil)
	c.unlockedAddChild(child)
}

func (c *IdemCloseChan) unlockedAddChild(child *IdemCloseChan) {
	if child == c {
		panic("cannot add ourselves as a child of ourselves; would deadlock")
	}
	if c.closed {
		child.unlockedCloseWithReason(c.whyClosed)
	}
	c.children = append(c.children, child)
	//vv("IdemCloseChan.AddChild() now has %v children", len(c.children))
}

func (c *IdemCloseChan) RemoveChild(child *IdemCloseChan) {
	lock(nil)
	defer unlock(nil)
	c.unlockedRemoveChild(child)
}
func (c *IdemCloseChan) unlockedRemoveChild(child *IdemCloseChan) {
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
	if child == h {
		panic("cannot add ourselves as a child of ourselves; would deadlock")
	}
	lock(nil)
	defer unlock(nil)

	h.children = append(h.children, child)
	h.ReqStop.unlockedAddChild(child.ReqStop)
}

// RemoveChild removes child from the set
// of h.children. It does not undo any
// close operation that AddChild did on addition.
func (h *Halter) RemoveChild(child *Halter) {
	lock(nil)
	defer unlock(nil)
	h.unlockedRemoveChild(child)
}
func (h *Halter) unlockedRemoveChild(child *Halter) {
	for i, ch := range h.children {
		if ch == child {
			h.children = append(h.children[:i], h.children[i+1:]...)
			return
		}
	}
	h.ReqStop.unlockedRemoveChild(child.ReqStop) // write race vs :364 read in Test408_multiple_circuits_open_and_close
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
func (h *Halter) StopTreeAndWaitTilDone(atMost time.Duration, giveup <-chan struct{}, why error) {
	// since ReqStop keeps a parallel tree, we only
	// need do this on the top level;
	// to efficiently stop traversal at first already
	// closed tree level.
	h.ReqStop.CloseWithReason(why)

	// We used to have to close our own Done channel.
	// Otherwise, waitTilDoneOrAtMost would block, waiting for it.
	// But/update: we added a flag to visit to allow skipping the
	// root. That way we can wait to close our own Done
	// until the very end, allowing a user to recursively
	// wait on a halter tree.
	h.waitTilDoneOrAtMost(atMost, giveup, false)

	h.Done.CloseWithReason(why)
}

// WaitTilDoneOrAtMost waits to return until
// all descendents have closed their Done.Chan or atMost
// time has elapsed. If atMost is <= 0, then we
// wait indefinitely for all the Done.Chan.
func (h *Halter) waitTilDoneOrAtMost(atMost time.Duration, giveup <-chan struct{}, visitSelf bool) {
	//defer vv("returning from waitTilDoneOrAtMost, h = %p", h)

	// a nil timeout channel will never fire in a select.
	var to <-chan time.Time
	// but we need a idem closed channel to handle
	// all possible reads of the timer by our children.
	timerGone := NewIdemCloseChan()
	if atMost > 0 {
		to = time.After(atMost)
	}
	//vv("in waitTilDoneOrAtMost, atMost = %v, visitSelf=%v in h Halter=%p", atMost, visitSelf, h)
	h.visit(visitSelf, func(y *Halter) {
		//vv("timeout in waitTilDoneOrAtMost t=%p, atMost = %v, visitSelf=%v in h Halter=%p", to, atMost, visitSelf, h)
		select { // hung here Test106TaskWait
		case <-y.Done.Chan:
		case <-to:
			timerGone.Close() // since timers are only good once:
		case <-timerGone.Chan:
		case <-giveup:
		}
	})
}

// visit calls f on each member of h's Halter tree in
// a pre-order traversal. f(h) is called,
// and then f(d) is called recusively on
// all descendants d of h. Children are
// visited while holding their parent's
// child mutex cmut, but the parent itself is not.
func (h *Halter) visit(visitSelf bool, f func(y *Halter)) {
	if visitSelf {
		f(h)
	}

	for _, child := range h.children {
		child.visit(true, f)
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
	lock(nil)
	defer unlock(nil)
	return c.unlockedWaitTilClosed(giveup)

}
func (c *IdemCloseChan) unlockedWaitTilClosed(giveup <-chan struct{}) (err error) {
	if c.closed {
		err = c.whyClosed
		return
	}
	// avoid data races if our advice above is ignored.
	bairn := append([]*IdemCloseChan{}, c.children...)
	// INVAR: we were open, and c.mut is now not held,
	// so we can be closed.
	for _, child := range bairn {
		err1 := child.unlockedWaitTilClosed(giveup)
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
		err, _ = c.unlockedReason()
		return
	case <-giveup:
		return ErrGiveUp
	}
}

// WaitTilChildrenClosed is just like WaitTilClosed, but
// does not require (or check) if we ourselves, the root
// of our tree, is closed.
func (c *IdemCloseChan) WaitTilChildrenClosed(giveup <-chan struct{}) (err error) {
	lock(nil)
	defer unlock(nil)
	return c.unlockedWaitTilChildrenClosed(giveup)
}
func (c *IdemCloseChan) unlockedWaitTilChildrenClosed(giveup <-chan struct{}) (err error) {

	for _, child := range c.children {
		child.unlockedWaitTilClosed(giveup)
	}
	for _, child := range c.children {
		err = child.unlockedFirstTreeReason()
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
	lock(nil)
	defer unlock(nil)
	return c.unlockedFirstTreeReason()
}

func (c *IdemCloseChan) unlockedFirstTreeReason() (err error) {

	why, isClosed := c.unlockedReason()
	if !isClosed {
		return ErrNotClosed
	}
	if why != nil {
		return why
	}

	for _, child := range c.children {
		err = child.unlockedFirstTreeReason()
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
func (c *IdemCloseChan) TaskAdd(delta int) (newval int) {
	lock(nil)
	defer unlock(nil)
	return c.unlockedTaskAdd(delta)
}
func (c *IdemCloseChan) unlockedTaskAdd(delta int) (newval int) {
	c.taskCount += delta
	newval = c.taskCount
	if c.taskCount <= 0 {
		//vv("taskCount is now %v, closing ourselves/all children.", c.taskCount)
		// inlined CloseWithReason, since we hold the mut.
		if !c.closed {
			c.whyClosed = ErrTasksAllDone
			close(c.Chan)
			c.closed = true
			for _, child := range c.children {
				child.unlockedCloseWithReason(c.whyClosed)
			}
		}
	} else {
		//vv("taskCount is now %v", c.taskCount)
	}
	return
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
func (c *IdemCloseChan) TaskDone() int {
	return c.TaskAdd(-1)
}
