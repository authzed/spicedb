package shared

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/dispatch"
)

func TestRewriteCanceledError(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(t.Context())
	cancelFunc()
	errorRewritten := RewriteError(ctx, ctx.Err(), nil)
	grpcutil.RequireStatus(t, codes.Canceled, errorRewritten)
}

func TestRewriteDeadlineExceededError(t *testing.T) {
	ctx, cancelFunc := context.WithDeadline(t.Context(), time.Now())
	defer cancelFunc()
	errorRewritten := RewriteError(ctx, ctx.Err(), nil)
	grpcutil.RequireStatus(t, codes.DeadlineExceeded, errorRewritten)
}

func TestRewriteMaximumDepthExceededError(t *testing.T) {
	errorRewritten := RewriteError(t.Context(), dispatch.NewMaxDepthExceededError(nil), &ConfigForErrors{
		MaximumAPIDepth: 50,
	})
	require.ErrorContains(t, errorRewritten, "See: https://spicedb.dev/d/debug-max-depth")
	grpcutil.RequireStatus(t, codes.ResourceExhausted, errorRewritten)
}
