package idem

import (
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

}
