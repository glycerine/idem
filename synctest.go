//go:build goexperiment.synctest

package idem

import (
	"sync"
	"testing/synctest"
	"time"
)

const globalUseSynctest bool = true

var waitMut sync.Mutex

var waitCond *sync.Cond = sync.NewCond(&waitMut)
var waitBegan time.Time
var waitEnded time.Time

func synctestWait_LetAllOtherGoroFinish() {
	synctest.Wait()
}

func bubbleOrNot(f func()) {
	synctest.Run(f)
}
