//go:build !ci

package datalayer

import "github.com/authzed/spicedb/pkg/datastore"

// trackHeldIterator returns the iterator unchanged outside of CI builds. In CI builds, it records
// the goroutine iterating over it as holding a datastore connection for the duration of the
// iteration, so that assertNoHeldIterator can detect reads issued while it is held.
func trackHeldIterator(_ string, it datastore.RelationshipIterator) datastore.RelationshipIterator {
	return it
}

// assertNoHeldIterator does nothing outside of CI builds. In CI builds, it panics if the calling
// goroutine is iterating over a relationships query, as the datastore read about to be issued would
// then require a second pooled connection while the first is held.
func assertNoHeldIterator(_ string) {}
