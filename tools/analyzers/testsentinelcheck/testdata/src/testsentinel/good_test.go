package testsentinel

import (
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
)

// This is a test file, so usage of NoSchemaHashForTesting is allowed
func TestSomething(t *testing.T) {
	hash := datastore.NoSchemaHashForTesting
	_ = hash
}
