package idem

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func Test101IdemCloseChan(t *testing.T) {
	bubbleOrNot(func() {
		// "IdemCloseChan should be safe for multiple Close()
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
	bubbleOrNot(func() {

		// IdemCloseChan typical usage pattern should function
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
	})
}

func Test103ChildClose(t *testing.T) {
	bubbleOrNot(func() {

		// IdemCloseChan AddChild should get closed when parent is.

		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)

		parent.Close()
		if child.IsClosed() {
			// good
		} else {
			panic("child should have been closed!")
		}

		if child2.IsClosed() {
			// good
		} else {
			panic("child2 should have been closed!")
		}

	})

	bubbleOrNot(func() {
		//after IdemCloseChan RemoveChild, the should not be closed when parent is.
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
			// good, child was not closed.
		}

		if child2.IsClosed() {
			panic("child2 should NOT have been closed!")
		} else {
			// good, child2 was not closed.
		}
	})

	bubbleOrNot(func() {
		// after IdemCloseChan RemoveChild, the should not be
		// closed, but the rest should be,  when parent is closed
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
			// good, child was not closed.
		}

		// child2 is still on the parent's list
		if child2.IsClosed() {
			// good, child2 was closed.
		} else {
			panic("child2 should have been closed!")
		}
	})

	bubbleOrNot(func() {
		// StopTree should recursively ReqStop all descendents.
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
		if got, want := len(seq), 6; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if got, want := len(seen), len(seq); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		// INVAR: no duplicates in seq, because can be none in seen,
		// and they are the same length.

		for _, h := range seq {
			if !h.ReqStop.IsClosed() {
				panic("child ReqStop should have been closed!")
			} else {
				// good child ReqStop was closed.
			}
		}
	})
}

func Test104WaitTilDone(t *testing.T) {
	bubbleOrNot(func() {
		// IdemCloseChan.WaitTilDone should return when the whole tree has closed, and with any reason set.

		parent := NewIdemCloseChan()
		child := NewIdemCloseChan()
		child2 := NewIdemCloseChan()
		parent.AddChild(child)
		parent.AddChild(child2)

		r1 := fmt.Errorf("reason1")
		parent.CloseWithReason(r1)

		reas, isClosed := child.Reason()
		if isClosed {
			// good
		} else {
			panic("child should have been closed!")
		}
		if got, want := reas, r1; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}

		reas2, isClosed2 := child.Reason()
		if isClosed2 {
			// good
		} else {
			panic("child2 should have been closed!")
		}
		if got, want := reas2, r1; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	bubbleOrNot(func() {
		// after IdemCloseChan RemoveChild, the should not be closed when parent is
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
			if reas != nil {
				t.Fatalf("expeced no reas as child not closed")
			}
		}

		reas2, isClosed2 := child2.Reason()
		if isClosed2 {
			panic("child2 should NOT have been closed!")
		} else {
			// good child2 was not closed.
			if reas2 != nil {
				t.Fatalf("expeced no reas2 as child not closed")
			}

		}
	})

	bubbleOrNot(func() {
		// after IdemCloseChan RemoveChild, the should not be closed, but the rest should be,  when parent is closed.
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
			if reas != nil {
				t.Fatalf("expeced no reas as child not closed")
			}
		}

		// child2 is still on the parent's list
		reas2, isClosed2 := child2.Reason()
		if isClosed2 {
			// good child2 was closed.
			if reas2 != r1 {
				t.Fatalf("expeced reas2 == r1 as child2 was closed")
			}
		} else {
			panic("child2 should have been closed!")
		}
	})

	bubbleOrNot(func() {
		// StopTree should recursively ReqStop all descendents.
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

		if got, want := len(seq), 6; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if got, want := len(seen), len(seq); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		// INVAR: no duplicates in seq, because can be none in seen,
		// and they are the same length.

		for _, h := range seq {
			reas, isClosed := h.ReqStop.Reason()
			if !isClosed {
				panic("child ReqStop should have been closed!")
			} else {
				// good child ReqStop was closed.
				if reas != r2 {
					t.Fatalf("expeced reas == r2")
				}
			}
		}
	})

	bubbleOrNot(func() {
		// WaitTilDone get an error set anywhere on the tree.
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

		if got, want := len(seq), 6; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if got, want := len(seen), len(seq); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		// INVAR: no duplicates in seq, because can be none in seen,
		// and they are the same length.

		err := root.ReqStop.WaitTilClosed(nil)
		if err != nil {
			t.Fatalf("expeced nil err")
		}

		err3 := greatgrandchild2.ReqStop.WaitTilClosed(nil)
		if err3 != r3 {
			t.Fatalf("expeced err3 == r3")
		}

		anyErr := root.ReqStop.FirstTreeReason()
		if anyErr != r3 {
			t.Fatalf("expeced anyErr == r3")
		}
	})

	bubbleOrNot(func() {
		// WaitTilChildrenDone does not root(ourselves) to be closed, but returns when all descendents have closed.
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
		if err != r3 {
			panic("wanted err == r3")
		}
		if root.ReqStop.IsClosed() {
			panic("wante root.ReqStop still open")
		}
	})
}

func Test105WaitTilChildrenDone(t *testing.T) {

	bubbleOrNot(func() {
		// WaitTilChildrenDone does not return until the sub-tree has all closed.
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
		if err != r3 {
			t.Fatalf("expeced err == r3")
		}
		if root.ReqStop.IsClosed() {
			t.Fatalf("expected root.ReqStop to be still open")
		}
	})
}

func Test106TaskWait(t *testing.T) {

	bubbleOrNot(func() {
		// TaskAdd, TaskDone, TaskWait are like a sync.WaitGroup that plays well with channels

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
					h.ReqStop.Close() // hung here Test106TaskWait
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

	bubbleOrNot(func() {
		// If we are interrupted with an error, all the worker pool still shuts down.

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
