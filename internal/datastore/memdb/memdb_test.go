package memdb

import (
	"testing"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
)

type memDBTest struct{}

func (mdbt memDBTest) New() (datastore.Datastore, error) {
	return NewMemdbDatastore(0)
}

func TestMemdbDatastore(t *testing.T) {
	test.TestAll(t, memDBTest{})
}
