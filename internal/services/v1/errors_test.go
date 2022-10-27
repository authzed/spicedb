package v1

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/grpcutil"
	"google.golang.org/grpc/codes"
)

func TestRewriteCanceledError(t *testing.T) {
	t.Parallel()
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	errorRewritten := rewriteError(ctx, ctx.Err())
	grpcutil.RequireStatus(t, codes.Canceled, errorRewritten)
}

func TestRewriteDeadlineExceededError(t *testing.T) {
	t.Parallel()
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now())
	defer cancelFunc()
	errorRewritten := rewriteError(ctx, ctx.Err())
	grpcutil.RequireStatus(t, codes.DeadlineExceeded, errorRewritten)
}
