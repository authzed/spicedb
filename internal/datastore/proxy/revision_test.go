package proxy

import (
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

// mustParseRevisionForTest parses a test revision string. It mirrors
// revisionparsing.MustParseRevisionForTest, which cannot be used here: that
// package imports the datastore engines, which import this package.
func mustParseRevisionForTest(revisionStr string) datastore.Revision {
	rev, err := revisions.RevisionParser(revisions.HybridLogicalClock)(revisionStr)
	if err != nil {
		panic(err)
	}
	return rev
}
