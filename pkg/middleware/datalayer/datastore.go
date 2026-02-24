package datalayer

import (
	"context"

	"github.com/authzed/spicedb/pkg/datalayer"
)

// FromContext reads the selected DataLayer out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) datalayer.DataLayer {
	return datalayer.FromContext(ctx)
}

// MustFromContext reads the selected DataLayer out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustFromContext(ctx context.Context) datalayer.DataLayer {
	dl := FromContext(ctx)
	if dl == nil {
		panic("datastore middleware did not inject datalayer")
	}

	return dl
}
