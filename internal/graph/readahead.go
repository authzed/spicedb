package graph

import (
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// readAheadRelationships returns an iterator over the relationships of it, which are read on a
// separate goroutine and buffered, without bound, as fast as the datastore returns them.
//
// SQL datastores hold a pooled connection until an iterator's results are fully read. Reading
// ahead releases that connection as soon as the datastore has returned every result, however
// long the consumer takes to process them (including any datastore access of its own), while
// still letting processing start with the first result.
//
// If the consumer stops early, reading stops at the next result. A panic while reading is
// re-raised on the consumer's goroutine.
func readAheadRelationships(it datastore.RelationshipIterator) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		var (
			mu       sync.Mutex
			ready    = sync.NewCond(&mu)
			buffer   []tuple.Relationship
			done     bool
			stopped  bool
			readErr  error
			panicked any
		)

		go func() {
			defer func() {
				r := recover()
				mu.Lock()
				done = true
				panicked = r
				mu.Unlock()
				ready.Signal()
			}()

			for rel, err := range it {
				mu.Lock()
				if stopped {
					mu.Unlock()
					return
				}
				if err != nil {
					readErr = err
					mu.Unlock()
					return
				}
				buffer = append(buffer, rel)
				mu.Unlock()
				ready.Signal()
			}
		}()

		defer func() {
			mu.Lock()
			stopped = true
			mu.Unlock()
		}()

		var batch []tuple.Relationship
		for {
			mu.Lock()
			for len(buffer) == 0 && !done {
				ready.Wait()
			}
			// Swap buffers, so the reader appends to the (emptied) previous batch while this one
			// is consumed.
			batch, buffer = buffer, batch[:0]
			finished, err, p := done, readErr, panicked
			mu.Unlock()

			for _, rel := range batch {
				if !yield(rel, nil) {
					return
				}
			}

			if finished {
				if p != nil {
					panic(p)
				}
				if err != nil {
					yield(tuple.Relationship{}, err)
				}
				return
			}
		}
	}
}
