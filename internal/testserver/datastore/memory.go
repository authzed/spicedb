//go:build docker
// +build docker

package datastore

import (
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
)

type memoryTest struct{}

// RunMemoryForTesting returns a RunningEngineForTest for the in-memory driver.
func RunMemoryForTesting(_ testing.TB) RunningEngineForTest {
	return &memoryTest{}
}

func (b *memoryTest) NewDatabase(_ testing.TB) string {
	// Does nothing.
	return ""
}

func (b *memoryTest) NewDatastore(_ testing.TB, initFunc InitFunc) datastore.Datastore {
	return initFunc("memory", "")
}
