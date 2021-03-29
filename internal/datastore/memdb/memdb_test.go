package memdb

import (
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
)

type memDBTest struct{}

func (mdbt memDBTest) New(revisionFuzzingTimedelta, gcWindow time.Duration) (datastore.Datastore, error) {
	return NewMemdbDatastore(0, revisionFuzzingTimedelta, gcWindow)
}

func TestMemdbDatastore(t *testing.T) {
	test.TestAll(t, memDBTest{})
}
