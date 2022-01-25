package datastore

import (
	"context"

	"github.com/authzed/spicedb/internal/datastore"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
)

// FromContext reads the selected datastore out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) datastore.Datastore {
	return datastoremw.FromContext(ctx)
}

// MustFromContext reads the selected datastore out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustFromContext(ctx context.Context) datastore.Datastore {
	dispatcher := FromContext(ctx)
	if dispatcher == nil {
		panic("datastore middleware did not inject datastore")
	}

	return dispatcher
}
