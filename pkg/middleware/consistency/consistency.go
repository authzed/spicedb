package consistency

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/middleware/consistency"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// RevisionFromContext reads the selected revision out of a context.Context and returns nil if it
// does not exist.
func RevisionFromContext(ctx context.Context) *decimal.Decimal {
	return consistency.RevisionFromContext(ctx)
}

// MustRevisionFromContext reads the selected revision out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustRevisionFromContext(ctx context.Context) (decimal.Decimal, *v1.ZedToken) {
	rev := consistency.RevisionFromContext(ctx)
	if rev == nil {
		panic("consistency middleware did not inject revision")
	}

	return *rev, zedtoken.NewFromRevision(*rev)
}
