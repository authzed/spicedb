package memdb

import (
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
)

type memDBTest struct{}

func (mdbt memDBTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	return NewMemdbDatastore(watchBufferLength, revisionFuzzingTimedelta, gcWindow, 0)
}

func TestMemdbDatastore(t *testing.T) {
	test.TestAll(t, memDBTest{})
}
