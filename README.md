# idem.Halter

idem.Halter supports a common pattern for halting goroutines in Go.

The test file halter_test.go is fairly
self explanatory. 

The summary is: Halter is used for shutting
down goroutines, and waiting until they
have exited. This requires cooperation
from the code the goroutine is running
of course, so you'll need to be writing
both. Halter has two idempotently closable
channels: ReqStop to request that the
goroutine stop, and Done for it to
acknowledge it is just about to exit.

It is really simple actually.
