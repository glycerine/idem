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


multiple versions cannot be supported
-------------------------------------

Warning: you must guarantee that only
a single version of this package is
used in your build. Do NOT mix a v1
version with any future v2, v3 version that comes around.

~~~
$ go version -m your_program
~~~

can be used to see included pacakges and
their versions.

This requirment must be preserved because we
depend on a package wide mutex to provide process
wide mutual exclusion across goroutines,
which dramatically reduces deadlock scenarios
when creating supervision trees.

Mixing a v1 and v2 in the same program
could result in two different packages,
using two different mutexes
in use by different goroutines that
join up in the same Halter or 
IdemCloseChan tree, and then
dragons will come and eat your entire
favorite music collection. 

Err, wait, no -- just deadlock, violated mutual exclusion
giving undetected data races, corrupt
memory, and the unpredictably crashing process;
if you are lucky.

We put a hack in singleton.go to try and
detect and bail in this situation, but better
to avoid it all together up front.

You have been warned.
