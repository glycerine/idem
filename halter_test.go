package idem

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test101IdemCloseChan(t *testing.T) {

	cv.Convey("IdemCloseChan should be safe for multiple Close()", t, func() {
		idem := NewIdemCloseChan()
		// it isn't close yet
		select {
		case <-idem.Chan:
			panic("already closed too early!")
		default:
		}
		idem.Close()
		idem.Close()
		idem.Close()
		<-idem.Chan
		cv.So(true, cv.ShouldEqual, true) // we should get here.
	})
}

type MyExample struct {
	Halt Halter
}

func NewMyExample() *MyExample {
	return &MyExample{
		Halt: *NewHalter(),
	}
}

func (m *MyExample) Stop() {
	m.Halt.ReqStop.Close()
	<-m.Halt.Done.Chan
}

func (m *MyExample) Start() {

	// typical m usage pattern
	go func() {
		for {
			select {
			// case(s) for other real work

			// case for shutdown:
			case <-m.Halt.ReqStop.Chan:
				// shutdown requested
				m.Halt.Done.Close()
				return
			}
		}
	}()
}

func Test102IdemCloseChanTypical(t *testing.T) {

	cv.Convey("IdemCloseChan typical usage pattern should function", t, func() {
		m := NewMyExample()
		m.Start()
		select {
		case <-time.After(100 * time.Millisecond):
		case <-m.Halt.Done.Chan:
			panic("closed Done too soon!")
		case <-m.Halt.ReqStop.Chan:
			panic("closed ReqStop too soon!")
		}
		m.Stop()
		<-m.Halt.Done.Chan
		cv.So(true, cv.ShouldEqual, true) // we should get here.
	})
}

func Test103ChildClose(t *testing.T) {

	cv.Convey("IdemCloseChan AddChild should get closed when parent is", t, func() {

		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)

		parent.Close()
		if child.IsClosed() {
			cv.So(true, cv.ShouldEqual, true)
		} else {
			panic("child should have been closed!")
		}

		if child2.IsClosed() {
			cv.So(true, cv.ShouldEqual, true)
		} else {
			panic("child2 should have been closed!")
		}

	})

	cv.Convey("after IdemCloseChan RemoveChild, the should not be closed when parent is", t, func() {
		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)
		parent.RemoveChild(child)
		parent.RemoveChild(child2)

		parent.Close()

		if child.IsClosed() {
			panic("child should NOT have been closed!")
		} else {
			// good child was not closed.
			cv.So(true, cv.ShouldEqual, true)
		}

		if child2.IsClosed() {
			panic("child2 should NOT have been closed!")
		} else {
			// good child2 was not closed.
			cv.So(true, cv.ShouldEqual, true)
		}
	})

	cv.Convey("after IdemCloseChan RemoveChild, the should not be closed, but the rest should be,  when parent is closed", t, func() {
		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)
		parent.RemoveChild(child)
		//skip: parent.RemoveChild(child2)

		parent.Close()

		if child.IsClosed() {
			panic("child should NOT have been closed!")
		} else {
			// good child was not closed.
			cv.So(true, cv.ShouldEqual, true)
		}

		// child2 is still on the parent's list
		if child2.IsClosed() {
			// good child2 was closed.
			cv.So(true, cv.ShouldEqual, true)
		} else {
			panic("child2 should have been closed!")
		}
	})

	cv.Convey("StopTree should recursively ReqStop all descendents", t, func() {
		root := NewHalter()
		child := NewHalter()
		child2 := NewHalter()
		grandchild := NewHalter()
		greatgrandchild1 := NewHalter()
		greatgrandchild2 := NewHalter()

		root.AddChild(child)
		root.AddChild(child2)
		child2.AddChild(grandchild)
		grandchild.AddChild(greatgrandchild1)
		grandchild.AddChild(greatgrandchild2)

		seen := make(map[*Halter]bool)
		var seq []*Halter
		root.visit(true, func(y *Halter) {
			y.ReqStop.Close()
			seen[y] = true
			seq = append(seq, y)
		})
		cv.So(len(seq), cv.ShouldEqual, 6)
		cv.So(len(seen), cv.ShouldEqual, len(seq))
		// INVAR: no duplicates in seq, because can be none in seen,
		// and they are the same length.

		for _, h := range seq {
			if !h.ReqStop.IsClosed() {
				panic("child ReqStop should have been closed!")
			} else {
				// good child ReqStop was closed.
				cv.So(true, cv.ShouldEqual, true)
			}
		}
	})

}

func Test104WaitTilDone(t *testing.T) {

	cv.Convey("IdemCloseChan.WaitTilDone should return when the whole tree has closed, and with any reason set.", t, func() {

		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)

		r1 := fmt.Errorf("reason1")
		parent.CloseWithReason(r1)

		reas, isClosed := child.Reason()
		if isClosed {
			cv.So(true, cv.ShouldEqual, true)
		} else {
			panic("child should have been closed!")
		}
		cv.So(reas, cv.ShouldEqual, r1)

		reas2, isClosed2 := child.Reason()
		if isClosed2 {
			cv.So(true, cv.ShouldEqual, true)
		} else {
			panic("child2 should have been closed!")
		}
		cv.So(reas2, cv.ShouldEqual, r1)
	})

	cv.Convey("after IdemCloseChan RemoveChild, the should not be closed when parent is", t, func() {
		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)
		parent.RemoveChild(child)
		parent.RemoveChild(child2)

		parent.Close()

		reas, isClosed := child.Reason()
		if isClosed {
			panic("child should NOT have been closed!")
		} else {
			// good child was not closed.
			cv.So(true, cv.ShouldEqual, true)
			cv.So(reas, cv.ShouldEqual, nil)
		}

		reas2, isClosed2 := child2.Reason()
		if isClosed2 {
			panic("child2 should NOT have been closed!")
		} else {
			// good child2 was not closed.
			cv.So(true, cv.ShouldEqual, true)
			cv.So(reas2, cv.ShouldEqual, nil)
		}
	})

	cv.Convey("after IdemCloseChan RemoveChild, the should not be closed, but the rest should be,  when parent is closed", t, func() {
		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)
		parent.RemoveChild(child)
		//skip: parent.RemoveChild(child2)

		r1 := fmt.Errorf("reason1")
		parent.CloseWithReason(r1)

		reas, isClosed := child.Reason()
		if isClosed {
			panic("child should NOT have been closed!")
		} else {
			// good child was not closed.
			cv.So(true, cv.ShouldEqual, true)
			cv.So(reas, cv.ShouldEqual, nil)
		}

		// child2 is still on the parent's list
		reas2, isClosed2 := child2.Reason()
		if isClosed2 {
			// good child2 was closed.
			cv.So(true, cv.ShouldEqual, true)
			cv.So(reas2, cv.ShouldEqual, r1)
		} else {
			panic("child2 should have been closed!")
		}
	})

	cv.Convey("StopTree should recursively ReqStop all descendents", t, func() {
		root := NewHalter()
		child := NewHalter()
		child2 := NewHalter()
		grandchild := NewHalter()
		greatgrandchild1 := NewHalter()
		greatgrandchild2 := NewHalter()

		root.AddChild(child)
		root.AddChild(child2)
		child2.AddChild(grandchild)
		grandchild.AddChild(greatgrandchild1)
		grandchild.AddChild(greatgrandchild2)

		r2 := fmt.Errorf("reason2")

		seen := make(map[*Halter]bool)
		var seq []*Halter
		root.visit(true, func(y *Halter) {
			y.ReqStop.CloseWithReason(r2)
			seen[y] = true
			seq = append(seq, y)
		})
		cv.So(len(seq), cv.ShouldEqual, 6)
		cv.So(len(seen), cv.ShouldEqual, len(seq))
		// INVAR: no duplicates in seq, because can be none in seen,
		// and they are the same length.

		for _, h := range seq {
			reas, isClosed := h.ReqStop.Reason()
			if !isClosed {
				panic("child ReqStop should have been closed!")
			} else {
				// good child ReqStop was closed.
				cv.So(true, cv.ShouldEqual, true)
				cv.So(reas, cv.ShouldEqual, r2)
			}
		}
	})

	cv.Convey("WaitTilDone get an error set anywhere on the tree", t, func() {
		root := NewHalter()
		child := NewHalter()
		child2 := NewHalter()
		grandchild := NewHalter()
		greatgrandchild1 := NewHalter()
		greatgrandchild2 := NewHalter()

		root.AddChild(child)
		root.AddChild(child2)
		child2.AddChild(grandchild)
		grandchild.AddChild(greatgrandchild1)
		grandchild.AddChild(greatgrandchild2)

		r3 := fmt.Errorf("reason3")

		seen := make(map[*Halter]bool)
		var seq []*Halter
		root.visit(true, func(y *Halter) {
			if y == greatgrandchild2 {
				y.ReqStop.CloseWithReason(r3)
			}
		})
		root.visit(true, func(y *Halter) {
			y.ReqStop.Close()
			seen[y] = true
			seq = append(seq, y)
		})

		cv.So(len(seq), cv.ShouldEqual, 6)
		cv.So(len(seen), cv.ShouldEqual, len(seq))
		// INVAR: no duplicates in seq, because can be none in seen,
		// and they are the same length.

		err := root.ReqStop.WaitTilClosed(nil)
		cv.So(err, cv.ShouldEqual, nil)
		err3 := greatgrandchild2.ReqStop.WaitTilClosed(nil)
		cv.So(err3, cv.ShouldEqual, r3)

		anyErr := root.ReqStop.FirstTreeReason()
		cv.So(anyErr, cv.ShouldEqual, r3)
	})

	cv.Convey("WaitTilChildrenDone does not root(ourselves) to be closed, but returns when all descendents have closed.", t, func() {
		root := NewHalter()
		child := NewHalter()
		child2 := NewHalter()
		grandchild := NewHalter()
		greatgrandchild1 := NewHalter()
		greatgrandchild2 := NewHalter()

		root.AddChild(child)
		root.AddChild(child2)
		child2.AddChild(grandchild)
		grandchild.AddChild(greatgrandchild1)
		grandchild.AddChild(greatgrandchild2)

		r3 := fmt.Errorf("reason3")

		seen := make(map[*Halter]bool)
		var seq []*Halter
		root.visit(true, func(y *Halter) {
			if y == greatgrandchild2 {
				y.ReqStop.CloseWithReason(r3)
			}
		})

		// close all BUT root
		root.visit(false, func(y *Halter) {
			if y == root {
				// skip!
			} else {
				y.ReqStop.Close()
			}
			seen[y] = true
			seq = append(seq, y)
		})

		err := root.ReqStop.WaitTilChildrenClosed(nil)
		cv.So(err, cv.ShouldEqual, r3)
		cv.So(root.ReqStop.IsClosed(), cv.ShouldEqual, false)
	})
}

func Test105WaitTilChildrenDone(t *testing.T) {

	cv.Convey("WaitTilChildrenDone does not return until the sub-tree has all closed. ", t, func() {
		root := NewHalter()
		child := NewHalter()
		child2 := NewHalter()
		grandchild := NewHalter()
		greatgrandchild1 := NewHalter()
		greatgrandchild2 := NewHalter()

		root.AddChild(child)
		root.AddChild(child2)
		child2.AddChild(grandchild)
		grandchild.AddChild(greatgrandchild1)
		grandchild.AddChild(greatgrandchild2)

		// we close in this order. So it must be bottom up.
		bairn := []*IdemCloseChan{
			greatgrandchild1.ReqStop,
			greatgrandchild2.ReqStop,
			grandchild.ReqStop,
			child.ReqStop,
			child2.ReqStop,
		}

		for i := range bairn {
			//vv("i = %v", i)
			if i > 0 {
				bairn[i-1].Close()
			}
			giveup := make(chan struct{})
			back := make(chan struct{})

			go func() {
				root.ReqStop.WaitTilChildrenClosed(giveup)
				close(back)
			}()
			select {
			case <-time.After(100 * time.Millisecond):
				// good, no close
			case <-back:
				panic("root.ReqStop.WaitTilChildrenDone returned too early")
			}
			close(giveup)
			<-back
			//vv("good: root.ReqStop.WaitTilChildrenDone gave up waiting")
		}

		// close the last
		r3 := fmt.Errorf("reason3")
		bairn[len(bairn)-1].CloseWithReason(r3)
		err := root.ReqStop.WaitTilChildrenClosed(nil)
		cv.So(err, cv.ShouldEqual, r3)
		cv.So(root.ReqStop.IsClosed(), cv.ShouldEqual, false)
	})

}

func Test106TaskWait(t *testing.T) {

	cv.Convey("TaskAdd, TaskDone, TaskWait are like a sync.WaitGroup that plays well with channels", t, func() {

		pool := NewHalter()

		// close down the whole pool iff
		// (all tasks are done or there is an error).
		workpool := 8 // extra workers we also want to see shutdown.
		nTask := 4

		var live atomic.Int64
		live.Add(int64(workpool))

		workCh := make(chan int)
		pool.ReqStop.TaskAdd(nTask)

		for worker := range workpool {
			h := NewHalter()
			pool.AddChild(h)
			go func(worker int, h *Halter) {
				defer func() {
					live.Add(-1)
					fmt.Printf("worker %v has finished.\n", worker)
					h.ReqStop.Close()
					h.Done.Close()
				}()
				for {
					select {
					case task := <-workCh:
						fmt.Printf("worker %v did task %v\n", worker, task)
						pool.ReqStop.TaskDone() // NOT h.TaskDone !
					case <-h.ReqStop.Chan:
						fmt.Printf("worker %v sees ReqStop closed\n", worker)
						return
					}
				}
			}(worker, h)
		}
		for i := range nTask {
			workCh <- i
		}
		vv("issued all %v tasks, now wait for them to finish", nTask)
		pool.ReqStop.TaskWait(nil)
		vv("pool.TaskWait has returned. Now shutdown the pool")
		//pool.ReqStop.Close()
		//pool.ReqStop.WaitTilChildrenClosed(nil)
		pool.StopTreeAndWaitTilDone(0, nil, nil)
		vv("The pool has shutdown.")
		left := live.Load()
		if left != 0 {
			t.Fatalf("expected no workers left, got %v", left)
		}
	})

	cv.Convey("If we are interrupted with an error, all the worker pool still shuts down.", t, func() {

		pool := NewHalter()

		// close down the whole pool iff
		// (all tasks are done or there is an error).
		workpool := 8 // extra workers we also want to see shutdown.
		nTask := 4

		var live atomic.Int64
		live.Add(int64(workpool))

		workCh := make(chan int)
		pool.ReqStop.TaskAdd(nTask)

		for worker := range workpool {
			h := NewHalter()
			pool.AddChild(h)
			go func(worker int, h *Halter) {
				defer func() {
					live.Add(-1)
					fmt.Printf("worker %v has finished.\n", worker)
					h.ReqStop.Close()
					h.Done.Close()
				}()
				for {
					select {
					case task := <-workCh:
						fmt.Printf("worker %v did task %v\n", worker, task)
						pool.ReqStop.TaskDone() // NOT h.TaskDone !
					case <-h.ReqStop.Chan:
						fmt.Printf("worker %v sees ReqStop closed\n", worker)
						return
					}
				}
			}(worker, h)
		}

		// sim: one less than all, then an error happens.
		for i := range nTask - 1 {
			workCh <- i
		}
		vv("issued all %v tasks, now wait for them to finish", nTask)
		time.Sleep(time.Millisecond)
		simulatedError := make(chan struct{})
		close(simulatedError)
		err := pool.ReqStop.TaskWait(simulatedError)
		vv("pool.TaskWait has returned, err = '%v' Now shutdown the pool", err)
		if err != ErrGiveUp {
			t.Fatalf("expected ErrGiveUp, got: '%v'", err)
		}
		//pool.ReqStop.Close()
		//pool.ReqStop.WaitTilChildrenClosed(nil)
		pool.StopTreeAndWaitTilDone(0, nil, nil)
		vv("The pool has shutdown.")
		left := live.Load()
		if left != 0 {
			t.Fatalf("expected no workers left, got %v", left)
		}
	})

}
