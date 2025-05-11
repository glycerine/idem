# idem.Halter

idem.Halter supports a common pattern for halting goroutines in Go.

Warning: you must guarantee that only
a single version of this package is
used in your build.

~~~
$ go version -m your_program
~~~

can be used to see included pacakges and
their versions. We try to check and if
we detect another instance, abort the
program. However this check is 
experimental and has not been
verified to work in all situations.

