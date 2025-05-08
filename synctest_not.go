//go:build !goexperiment.synctest

package idem

const globalUseSynctest bool = false

func synctestWait_LetAllOtherGoroFinish() {}

func bubbleOrNot(f func()) {
	f()
}
