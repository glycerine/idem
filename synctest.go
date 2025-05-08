//go:build goexperiment.synctest

package idem

import (
	"testing/synctest"
)

const globalUseSynctest bool = true

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}

func bubbleOrNot(f func()) {
	synctest.Run(f)
}
