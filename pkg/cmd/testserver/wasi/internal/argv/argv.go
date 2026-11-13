// Package argv gives the process a program name when the WASI host provides no arguments.
//
// Several dependencies read os.Args[0] during package initialization (for example, pflag), and
// panic when a host instantiates the module without arguments, as hosts of reactor modules
// commonly do. Go initializes ready packages in import path order, and this package depends only
// on os, so it is initialized before any package importing os whose import path sorts after it,
// including those dependencies.
package argv

import "os"

func init() {
	if len(os.Args) == 0 {
		os.Args = []string{"spicedb"}
	}
}
