package shared

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func TestRewriteError(t *testing.T) {
	t.Parallel()

	ctxCancelWithCause, cancel := context.WithCancelCause(t.Context())
	cancel(fmt.Errorf("look at me i canceled this"))

	ctxCancelWithCauseGrpc, cancel2 := context.WithCancelCause(t.Context())
	cancel2(status.Error(codes.AlreadyExists, "already exists"))

	tests := []struct {
		name             string
		inputCtx         context.Context
		inputError       error
		config           *ConfigForErrors
		expectedCode     codes.Code
		expectedContains string
	}{
		// context
		{
			name:             "context canceled without cause",
			inputError:       context.Canceled,
			config:           nil,
			expectedCode:     codes.Canceled,
			expectedContains: "context canceled",
		},
		{
			name:             "context canceled with cause",
			inputCtx:         ctxCancelWithCause,
			inputError:       context.Canceled,
			config:           nil,
			expectedCode:     codes.Canceled,
			expectedContains: "look at me i canceled this",
		},
		{
			name:             "context canceled with cause and embedded grpc error",
			inputCtx:         ctxCancelWithCauseGrpc,
			inputError:       context.Canceled,
			config:           nil,
			expectedCode:     codes.AlreadyExists,
			expectedContains: "already exists",
		},
		{
			name:             "context deadline exceeded error",
			inputError:       context.DeadlineExceeded,
			config:           nil,
			expectedCode:     codes.DeadlineExceeded,
			expectedContains: "context deadline exceeded",
		},
		// dispatch
		{
			name:       "maximum depth exceeded error",
			inputError: dispatch.NewMaxDepthExceededError(nil),
			config: &ConfigForErrors{
				MaximumAPIDepth: 50,
			},
			expectedCode:     codes.ResourceExhausted,
			expectedContains: "max depth exceeded: this usually indicates a recursive or too deep data dependency. See https://spicedb.dev/d/debug-max-depth",
		},
		// pool
		{
			name:             "error acquiring connections",
			inputError:       pool.ErrAcquire,
			config:           nil,
			expectedCode:     codes.ResourceExhausted,
			expectedContains: "consider increasing write pool size and/or datastore capacity",
		},
		{
			name:             "acquisition error",
			inputError:       pool.ErrAcquire,
			config:           nil,
			expectedCode:     codes.ResourceExhausted,
			expectedContains: "failed to acquire in time: consider increasing write pool size and/or datastore capacity",
		},
		// datastore
		{
			name:             "watch disconnected",
			inputError:       datastore.NewWatchDisconnectedErr(),
			config:           nil,
			expectedCode:     codes.ResourceExhausted,
			expectedContains: "watch fell too far behind and was disconnected; consider increasing watch buffer size via the flag --datastore-watch-buffer-length",
		},
		{
			name:             "watch canceled",
			inputError:       datastore.NewWatchCanceledErr(),
			config:           nil,
			expectedCode:     codes.Canceled,
			expectedContains: "watch canceled by user: watch was canceled by the caller",
		},
		{
			name:             "watch disabled",
			inputError:       datastore.NewWatchDisabledErr("it was disabled"),
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "watch is currently disabled: it was disabled",
		},
		{
			name:             "watch retryable",
			inputError:       datastore.NewWatchTemporaryErr(errors.New("server is shutting down")),
			config:           nil,
			expectedCode:     codes.Unavailable,
			expectedContains: "watch has failed with a temporary condition: server is shutting down. Please retry",
		},
		{
			name:             "counter already registered",
			inputError:       datastore.NewCounterAlreadyRegisteredErr("somecounter", &corev1.RelationshipFilter{ResourceType: "doc"}),
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "counter with name `somecounter` already registered",
		},
		{
			name:             "counter not registered",
			inputError:       datastore.NewCounterNotRegisteredErr("somecounter"),
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "counter with name `somecounter` not found",
		},
		{
			name:             "datastore readonly",
			inputError:       datastore.NewReadonlyErr(),
			config:           nil,
			expectedCode:     codes.Unavailable,
			expectedContains: "service read-only",
		},
		{
			name:             "datastore invalid revision",
			inputError:       datastore.NewInvalidRevisionErr(datastore.NoRevision, datastore.RevisionStale),
			config:           nil,
			expectedCode:     codes.OutOfRange,
			expectedContains: "invalid zedtoken",
		},
		{
			name:             "datastore caveat not found",
			inputError:       datastore.NewCaveatNameNotFoundErr("somecaveat"),
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "caveat with name `somecaveat` not found",
		},
		// graph
		{
			name:             "graph unimplemented",
			inputError:       graph.NewUnimplementedErr(fmt.Errorf("unimplemented")),
			config:           nil,
			expectedCode:     codes.Unimplemented,
			expectedContains: "unimplemented",
		},
		{
			name:             "graph always fail",
			inputError:       graph.NewAlwaysFailErr(),
			config:           nil,
			expectedCode:     codes.Internal,
			expectedContains: "internal error: always fail",
		},
		{
			name:             "graph missing type info",
			inputError:       graph.NewRelationMissingTypeInfoErr("document", "view"),
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "failed precondition: relation/permission `view` under definition `document` is missing type information",
		},
		// { TODO this doesn't pass
		//	name: "compiler error",
		//	inputError: &compiler.WithContextError{
		//		BaseCompilerError: compiler.BaseCompilerError{
		//			BaseMessage: "syntax error in schema",
		//		},
		//	},
		//	config:           nil,
		//	expectedCode:     codes.InvalidArgument,
		//	expectedContains: "syntax error in schema",
		// },
		{
			name: "source error",
			inputError: spiceerrors.NewWithSourceError(
				fmt.Errorf("invalid schema definition"),
				"definition document {\n  relation viewer: user\n}",
				1,
				1,
			),
			config:           nil,
			expectedCode:     codes.InvalidArgument,
			expectedContains: "invalid schema definition",
		},
		// cursor
		{
			name:             "cursor hash mismatch",
			inputError:       cursor.ErrHashMismatch,
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "the cursor provided does not have the same arguments as the original API call",
		},
		// schema errors
		{
			name:             "namespace not found",
			inputError:       namespace.NewNamespaceNotFoundErr("document"),
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "object definition `document` not found",
		},
		{
			name:             "relation not found",
			inputError:       namespace.NewRelationNotFoundErr("document", "viewer"),
			config:           nil,
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "relation/permission `viewer` not found under definition `document`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.inputCtx != nil {
				ctx = tt.inputCtx
			}

			errorRewritten := RewriteError(ctx, tt.inputError, tt.config)
			require.NotNil(t, errorRewritten)
			grpcutil.RequireStatus(t, tt.expectedCode, errorRewritten)
			t.Log(errorRewritten.Error())
			if tt.expectedContains != "" {
				require.ErrorContains(t, errorRewritten, tt.expectedContains)
			}
		})
	}
}
