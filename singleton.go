package idem

import (
	"fmt"
	"os"
)

var (
	// Use an env var to detect multiple instances of this package.

	// Warning: these two strings must never be changed, ever.
	// They must be set in stone for the singleton check to work.
	envSingletonPrefix = "GOLANG_SINGLETON_PACKAGE_REQUIRED_"

	thisPackageName = "github.com/glycerine/idem"
)

// We must insure that two versions of the idem package
// are not in use, otherwise they will have different
// idem.mut variables, and deadlocks/violations of
// mutual exclusion will occur. We check for this
// and exit after priting to os.Stderr, if found.
//
// We exploit the fact that all package init func are
// run sequentially by a single goroutine, and
// that the process env var space is shared by
// by all goroutines.
func init() {

	// Check if another package version is present.
	nm := envSingletonPrefix + thisPackageName
	existing := os.Getenv(nm)

	if existing == "" {
		// No existing instance, we're the first,
		// and hopefully only, one.
		os.Setenv(nm, "claimed")
	} else {
		// Another instance exists! crash with error.
		fmt.Fprintf(os.Stderr, `
%v FATAL error: env var 
    %v 
has already been claimed. Multiple instances of this package,
    %v
are present in this build -- this will produces deadlocks
and mutual exclusion violations, and cannot be supported. 

Run 'go version -m your_program' on this binary to
see the package versions included, and insure
only a single version of this package is used;
adjust your dependencies and go.mod file.

Exiting now for safety.
`, fileLine(2), nm, thisPackageName)

		os.Exit(1)
	}
}
