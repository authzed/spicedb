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
	t.Run("TestSimple", func(t *testing.T) { test.TestSimple(t, &memDBTest{}) })
	t.Run("TestWatch", func(t *testing.T) { test.TestWatch(t, &memDBTest{}) })
	t.Run("TestWatchCancel", func(t *testing.T) { test.TestWatchCancel(t, &memDBTest{}) })
	t.Run("TestDelete", func(t *testing.T) { test.TestDelete(t, &memDBTest{}) })
}
