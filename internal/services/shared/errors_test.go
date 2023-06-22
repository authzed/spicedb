package shared

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/dispatch"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestRewriteCanceledError(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	errorRewritten := RewriteError(ctx, ctx.Err(), nil)
	grpcutil.RequireStatus(t, codes.Canceled, errorRewritten)
}

func TestRewriteDeadlineExceededError(t *testing.T) {
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now())
	defer cancelFunc()
	errorRewritten := RewriteError(ctx, ctx.Err(), nil)
	grpcutil.RequireStatus(t, codes.DeadlineExceeded, errorRewritten)
}

func TestRewriteMaximumDepthExceededError(t *testing.T) {
	errorRewritten := RewriteError(context.Background(), dispatch.NewMaxDepthExceededError(nil), &ConfigForErrors{
		MaximumAPIDepth: 50,
	})
	require.ErrorContains(t, errorRewritten, "See: https://spicedb.dev/d/debug-max-depth")
	grpcutil.RequireStatus(t, codes.ResourceExhausted, errorRewritten)
}

func TestRewriteMaximumDepthExceededErrorForCheck(t *testing.T) {
	errorRewritten := RewriteError(context.Background(), dispatch.NewMaxDepthExceededError(&dispatchv1.DispatchCheckRequest{}), &ConfigForErrors{
		MaximumAPIDepth: 50,
	})
	require.ErrorContains(t, errorRewritten, "See: https://spicedb.dev/d/debug-max-depth-check")
	require.ErrorContains(t, errorRewritten, "--explain")
	grpcutil.RequireStatus(t, codes.ResourceExhausted, errorRewritten)
}
