package consistency

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/middleware/consistency"
	"github.com/authzed/spicedb/pkg/datastore"
)

// RevisionFromContext reads the selected revision out of a context.Context, computes a zedtoken
// from it, and returns an internal error if it has not been set on the context.
func RevisionFromContext(ctx context.Context) (datastore.Revision, *v1.ZedToken, error) {
	return consistency.RevisionFromContext(ctx)
}
