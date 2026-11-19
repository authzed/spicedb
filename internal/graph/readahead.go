package graph

import (
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// readAheadRelationships returns an iterator over the relationships returned by query, starting
// after the given cursor. The relationships are read on a separate goroutine and buffered as fast
// as the datastore returns them, up to maxBuffered relationships that have not yet been consumed.
//
// SQL datastores hold a pooled connection until an iterator's results are fully read. Reading
// ahead releases that connection as soon as the datastore has returned every result, however
// long the consumer takes to process them (including any datastore access of its own), while
// still letting processing start with the first result.
//
// If the buffer fills, the query is closed, releasing its connection, and reading resumes with a
// new query after the last relationship read, once the consumer has worked through half of the
// buffer. query must therefore return relationships in a stable order that the cursor resumes.
//
// If the consumer stops early, reading stops at the next result.
func readAheadRelationships(
	after options.Cursor,
	maxBuffered int,
	query func(after options.Cursor) (datastore.RelationshipIterator, error),
) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		var (
			mu      sync.Mutex
			changed = sync.NewCond(&mu)

			// buffer holds relationships read but not yet taken by the consumer, and inFlight
			// counts those taken but not yet processed; together they are bounded by maxBuffered.
			buffer   []tuple.Relationship
			inFlight int
			done     bool
			stopped  bool
			readErr  error
		)

		go func() {
			defer func() {
				mu.Lock()
				done = true
				mu.Unlock()
				changed.Broadcast()
			}()

			cursor := after
			for {
				it, err := query(cursor)
				if err != nil {
					mu.Lock()
					readErr = err
					mu.Unlock()
					return
				}

				full := false
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
					full = len(buffer)+inFlight >= maxBuffered
					mu.Unlock()
					changed.Broadcast()

					if full {
						// Closes the query, releasing its connection, to resume after this relationship.
						cursor = options.ToCursor(rel)
						break
					}
				}

				if !full {
					return
				}

				// Wait for the consumer to catch up before resuming with a new query.
				mu.Lock()
				for len(buffer)+inFlight > maxBuffered/2 && !stopped {
					changed.Wait()
				}
				wasStopped := stopped
				mu.Unlock()
				if wasStopped {
					return
				}
			}
		}()

		defer func() {
			mu.Lock()
			stopped = true
			mu.Unlock()
			changed.Broadcast()
		}()

		var batch []tuple.Relationship
		for {
			mu.Lock()
			for len(buffer) == 0 && !done {
				changed.Wait()
			}
			// Swap buffers, so the reader appends to the (emptied) previous batch while this one
			// is consumed.
			batch, buffer = buffer, batch[:0]
			inFlight = len(batch)
			finished, err := done, readErr
			mu.Unlock()

			for i, rel := range batch {
				if !yield(rel, nil) {
					return
				}

				// Periodically release consumed relationships from the bound, so that a paused
				// reader can resume before the whole batch has been processed.
				if remaining := len(batch) - i - 1; remaining%1024 == 0 {
					mu.Lock()
					inFlight = remaining
					mu.Unlock()
					changed.Broadcast()
				}
			}

			if finished {
				if err != nil {
					yield(tuple.Relationship{}, err)
				}
				return
			}
		}
	}
}
