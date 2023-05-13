package shared

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/grpcutil"
	"google.golang.org/grpc/codes"
)

func TestRewriteCanceledError(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	errorRewritten := RewriteError(ctx, ctx.Err())
	grpcutil.RequireStatus(t, codes.Canceled, errorRewritten)
}

func TestRewriteDeadlineExceededError(t *testing.T) {
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now())
	defer cancelFunc()
	errorRewritten := RewriteError(ctx, ctx.Err())
	grpcutil.RequireStatus(t, codes.DeadlineExceeded, errorRewritten)
}
