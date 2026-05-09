// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

import (
	"fmt"
	"log"
)

// Error returns the error status of the BDD.
func (b *BDD) Error() string {
	if b.error == nil {
		return ""
	}
	return b.error.Error()
}

// Errored returns true if there was an error during a computation.
func (b *BDD) Errored() bool {
	return b.error != nil
}

func (b *BDD) seterror(format string, a ...interface{}) Node {
	if b.error != nil {
		format = format + "; " + b.Error()
		b.error = fmt.Errorf(format, a...)
		return nil
	}
	b.error = fmt.Errorf(format, a...)
	if _DEBUG {
		log.Println(b.error)
	}
	return nil
}
