//go:build ci

package datalayer

import (
	"fmt"
	"strings"
	"sync"

	"github.com/authzed/spicedb/internal/goroutineid"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// heldIterators records, per goroutine, the operations whose relationship iterators that
// goroutine is currently iterating.
var heldIterators = struct {
	sync.Mutex
	byGoroutine map[uint64][]string
}{byGoroutine: map[uint64][]string{}}

func trackHeldIterator(operation string, it datastore.RelationshipIterator) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		assertNoHeldIterator(operation)

		gid := goroutineid.MustCurrent()
		heldIterators.Lock()
		heldIterators.byGoroutine[gid] = append(heldIterators.byGoroutine[gid], operation)
		heldIterators.Unlock()

		defer func() {
			heldIterators.Lock()
			held := heldIterators.byGoroutine[gid]
			if len(held) <= 1 {
				delete(heldIterators.byGoroutine, gid)
			} else {
				heldIterators.byGoroutine[gid] = held[:len(held)-1]
			}
			heldIterators.Unlock()
		}()

		for rel, err := range it {
			if !yield(rel, err) {
				return
			}
		}
	}
}

func assertNoHeldIterator(operation string) {
	gid := goroutineid.MustCurrent()
	heldIterators.Lock()
	held := strings.Join(heldIterators.byGoroutine[gid], ", ")
	heldIterators.Unlock()

	if held != "" {
		panic(fmt.Sprintf(
			"datastore read %s was issued while iterating over the results of %s: SQL datastores hold a "+
				"pooled connection for the duration of an iteration, so this requests a second connection "+
				"while holding the first, which can deadlock the connection pool "+
				"(https://github.com/authzed/spicedb/issues/3433); read everything needed before iterating, "+
				"or collect the iterator's results first",
			operation, held,
		))
	}
}
