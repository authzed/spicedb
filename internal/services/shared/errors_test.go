package shared

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func TestRewriteError(t *testing.T) {
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
			expectedCode:     codes.FailedPrecondition,
			expectedContains: "max depth exceeded: this usually indicates a recursive or too deep data dependency. See " + sharederrors.MaxDepthErrorLink,
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
			ctx := t.Context()
			if tt.inputCtx != nil {
				ctx = tt.inputCtx
			}

			errorRewritten := RewriteError(ctx, tt.inputError, tt.config)
			require.Error(t, errorRewritten)
			grpcutil.RequireStatus(t, tt.expectedCode, errorRewritten)
			t.Log(errorRewritten.Error())
			if tt.expectedContains != "" {
				require.ErrorContains(t, errorRewritten, tt.expectedContains)
			}
		})
	}
}

func TestRewriteErrorSanitizesDatastoreDriverErrors(t *testing.T) {
	// Datastore-internal substrings that must never appear in a client-facing message.
	internalSubstrings := []string{"SQLSTATE", "COPY", "57014", "23505", "40001", "PermissionDenied", "pg_", "Deadlock", "1213", "restart transaction", "TransactionRetry"}

	// CockroachDB uses the pgx driver, so its raw errors are *pgconn.PgError. The retry pool
	// additionally wraps them in engine-specific error types; all of them must be sanitized.
	crdbRetryErr := &pgconn.PgError{
		Severity: "ERROR",
		Code:     "40001",
		Message:  "restart transaction: TransactionRetryWithProtoRefreshError: retry txn",
	}

	tests := []struct {
		name         string
		inputError   error
		expectedCode codes.Code
	}{
		{
			name: "postgres COPY-abort wrapper (query canceled)",
			inputError: &pgconn.PgError{
				Severity: "ERROR",
				Code:     "57014",
				Message:  "COPY from stdin failed: rpc error: code = PermissionDenied desc = unauthorized operation",
			},
			expectedCode: codes.Canceled,
		},
		{
			name: "postgres serialization failure",
			inputError: &pgconn.PgError{
				Severity: "ERROR",
				Code:     "40001",
				Message:  "could not serialize access due to concurrent update",
			},
			expectedCode: codes.Aborted,
		},
		{
			name: "postgres unexpected error",
			inputError: &pgconn.PgError{
				Severity: "ERROR",
				Code:     "42P01",
				Message:  `relation "relation_tuple" does not exist`,
			},
			expectedCode: codes.Internal,
		},
		{
			name:         "mysql deadlock",
			inputError:   &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"},
			expectedCode: codes.Aborted,
		},
		{
			name:         "mysql unexpected error",
			inputError:   &mysql.MySQLError{Number: 1146, Message: "Table 'spicedb.relation_tuple' doesn't exist"},
			expectedCode: codes.Internal,
		},
		{
			name:         "cockroachdb bare serialization failure",
			inputError:   crdbRetryErr,
			expectedCode: codes.Aborted,
		},
		{
			name:         "cockroachdb error wrapped in MaxRetryError",
			inputError:   &pool.MaxRetryError{MaxRetries: 3, LastErr: crdbRetryErr},
			expectedCode: codes.Aborted,
		},
		{
			name:         "cockroachdb error wrapped in ResettableError",
			inputError:   &pool.ResettableError{Err: crdbRetryErr},
			expectedCode: codes.Unavailable,
		},
		{
			name:         "cockroachdb error wrapped in RetryableError",
			inputError:   &pool.RetryableError{Err: crdbRetryErr},
			expectedCode: codes.Unavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rewritten := RewriteError(t.Context(), tt.inputError, nil)
			require.Error(t, rewritten)
			grpcutil.RequireStatus(t, tt.expectedCode, rewritten)

			st, ok := status.FromError(rewritten)
			require.True(t, ok, "sanitized error must be a gRPC status")
			for _, internal := range internalSubstrings {
				require.NotContains(t, st.Message(), internal, "sanitized message must not contain datastore internals")
			}
		})
	}
}
