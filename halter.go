package idem

import (
	"fmt"
	//"sync"
	"time"
)

// to avoid locking/deadlock issues, processes
// including this package get exactly one
// process-global single channel-based lock for all Halter
// and IdemCloseChan tree operations.
var globalTreeLock *globalSingleLock

type canLock interface {
	holderChan() *IdemCloseChan
	holderHalt() *Halter
}

func (s *IdemCloseChan) holderChan() *IdemCloseChan { return s }
func (s *IdemCloseChan) holderHalt() *Halter        { return nil }
func (s *Halter) holderChan() *IdemCloseChan        { return nil }
func (s *Halter) holderHalt() *Halter               { return s }

type globalSingleLock struct {
	// empty channel means unlocked.
	lockCh chan canLock
}

func init() {
	s := &globalSingleLock{
		lockCh: make(chan canLock, 1),
	}
	globalTreeLock = s
}

// lock returns true if it got the lock,
// false if it bailed beforehand.
func lock(bail chan struct{}, me canLock) bool {
	select {
	case globalTreeLock.lockCh <- me:
		return true
	case <-bail:
		return false
	}
}

// unlock does not block if the lock is
// already unlocked (return nil).
// Otherwise returns who held the lock.
func unlock() (hadIt canLock) {
	select {
	case hadIt = <-globalTreeLock.lockCh:
	default:
		// already unlocked, return nil
	}
	return
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
	// was hung on lock here in Test106TaskWait, before chan based lock
	lock(nil, c)
	defer unlock()
	return c.nolockingClose()
}

var ErrBailed = fmt.Errorf("bail chan closed before getting the lock")

//func (c *IdemCloseChan) CloseOrBail(bail chan struct{}) (err error, bailed bool) {
//	if !lock(bail) {
//		return ErrBailed, true
//	}
//	defer unlock()
//	return c.nolockingClose(), false
//}

func (c *IdemCloseChan) nolockingClose() error {
	if !c.closed {
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.nolockingClose()
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
	lock(nil, c)
	defer unlock()
	return c.nolockingCloseWithReason(why)
}

func (c *IdemCloseChan) nolockingCloseWithReason(why error) error {
	if !c.closed {
		c.whyClosed = why
		close(c.Chan)
		c.closed = true
		for _, child := range c.children {
			child.nolockingCloseWithReason(why)
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
	lock(nil, c)
	defer unlock()
	return c.nolockingReason()
}

func (c *IdemCloseChan) nolockingReason() (why error, isClosed bool) {
	why = c.whyClosed
	isClosed = c.closed
	return
}

// Reason1 is the same as Reason but without the isClosed.
// This is easier to use in logging.
func (c *IdemCloseChan) Reason1() (why error) {
	lock(nil, c)
	defer unlock()
	return c.nolockingReason1()
}

func (c *IdemCloseChan) nolockingReason1() (why error) {
	why = c.whyClosed
	return
}

// IsClosed tells you if Chan is already closed or not.
func (c *IdemCloseChan) IsClosed() bool {
	lock(nil, c)
	defer unlock()
	return c.closed
}

func (c *IdemCloseChan) nolockingIsClosed() bool {
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
	lock(nil, c)
	defer unlock()
	c.nolockingAddChild(child)
}

func (c *IdemCloseChan) nolockingAddChild(child *IdemCloseChan) {
	if child == c {
		panic("cannot add ourselves as a child of ourselves; would deadlock")
	}
	if c.closed {
		child.nolockingCloseWithReason(c.whyClosed)
	}
	c.children = append(c.children, child)
	//vv("IdemCloseChan.AddChild() now has %v children", len(c.children))
}

func (c *IdemCloseChan) RemoveChild(child *IdemCloseChan) {
	lock(nil, c)
	defer unlock()
	c.nolockingRemoveChild(child)
}
func (c *IdemCloseChan) nolockingRemoveChild(child *IdemCloseChan) {
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
	lock(nil, h)
	defer unlock()

	h.children = append(h.children, child)
	h.ReqStop.nolockingAddChild(child.ReqStop)
}

// RemoveChild removes child from the set
// of h.children. It does not undo any
// close operation that AddChild did on addition.
func (h *Halter) RemoveChild(child *Halter) {
	lock(nil, h)
	defer unlock()
	h.nolockingRemoveChild(child)
}
func (h *Halter) nolockingRemoveChild(child *Halter) {
	for i, ch := range h.children {
		if ch == child {
			h.children = append(h.children[:i], h.children[i+1:]...)
			return
		}
	}
	h.ReqStop.nolockingRemoveChild(child.ReqStop) // write race vs :364 read in Test408_multiple_circuits_open_and_close
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
// all descendants d of h.
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

	// true here means close ourselves before returning.
	err = c.helperWaitTilClosed(giveup, true)
	if err != nil {
		return
	}
	for _, child := range c.children {
		err = child.FirstTreeReason()
		if err != nil {
			return err
		}
	}
	return
}

// PRE: the lock is not held
func (c *IdemCloseChan) helperWaitTilClosed(giveup <-chan struct{}, closeSelf bool) (err error) {
	why, isClosed := c.Reason()
	if isClosed {
		err = why
		return
	}
	// a little bit of protection against simultaneous mutation
	lock(nil, c)
	bairn := append([]*IdemCloseChan{}, c.children...)
	unlock()

	for _, child := range bairn {
		err1 := child.helperWaitTilClosed(giveup, true)
		// first error is sticky
		if err == nil {
			err = err1
		}
	}
	if closeSelf {
		// wait for our own close.
		select {
		case <-c.Chan:
			// first error is still sticky, don't overwrite with c.Reason().
			if err != nil {
				return
			}
			if err == nil {
				err = c.FirstTreeReason()
			}
			return
		case <-giveup:
			return ErrGiveUp
		}
	}
	return
}

// WaitTilChildrenClosed is just like WaitTilClosed, but
// does not require (or check) if we ourselves, the root
// of our tree, is closed.
func (c *IdemCloseChan) WaitTilChildrenClosed(giveup <-chan struct{}) (err error) {

	err = c.helperWaitTilClosed(giveup, false)
	if err != nil {
		return
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
	lock(nil, c)
	defer unlock()
	return c.nolockingFirstTreeReason()
}

func (c *IdemCloseChan) nolockingFirstTreeReason() (err error) {

	why, isClosed := c.nolockingReason()
	if !isClosed {
		return ErrNotClosed
	}
	if why != nil {
		return why
	}

	for _, child := range c.children {
		err = child.nolockingFirstTreeReason()
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
	lock(nil, c)
	defer unlock()
	return c.nolockingTaskAdd(delta)
}
func (c *IdemCloseChan) nolockingTaskAdd(delta int) (newval int) {
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
				child.nolockingCloseWithReason(c.whyClosed)
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
