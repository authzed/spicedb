package testsentinel

import (
	"github.com/authzed/spicedb/pkg/datastore"
)

// This is NOT a test file, so usage of NoSchemaHashForTesting should be flagged
func SomeFunction() {
	hash := datastore.NoSchemaHashForTesting // want "NoSchemaHashForTesting should only be used in test files"
	_ = hash
}
