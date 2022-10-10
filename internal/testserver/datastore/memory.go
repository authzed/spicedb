package datastore

import (
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
)

type memoryTest struct{}

// RunMemoryForTesting returns a RunningEngineForTest for the in-memory driver.
func RunMemoryForTesting(t testing.TB) RunningEngineForTest {
	return &memoryTest{}
}

func (b *memoryTest) NewDatabase(t testing.TB) string {
	// Does nothing.
	return ""
}

func (b *memoryTest) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	return initFunc("memory", "")
}
