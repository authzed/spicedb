package development

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
)

func TestNewDevContextWithInvalidSchema(t *testing.T) {
	_, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `invalid schema syntax`,
	})

	require.NoError(t, err)
	require.NotNil(t, devErrs)
	require.Len(t, devErrs.InputErrors, 1)
	require.Equal(t, devinterface.DeveloperError_SCHEMA_ISSUE, devErrs.InputErrors[0].Kind)
}

func TestNewDevContextWithInvalidRelationship(t *testing.T) {
	_, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}
definition document {
	relation viewer: user
}`,
		Relationships: []*core.RelationTuple{
			{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "nonexistent",
					ObjectId:  "someid",
					Relation:  "somerel",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "someuser",
					Relation:  "...",
				},
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, devErrs)
	require.Len(t, devErrs.InputErrors, 1)
	require.Equal(t, devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE, devErrs.InputErrors[0].Kind)
}

func TestDevContextDispose(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}`,
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	// Test dispose
	devCtx.Dispose()

	// Test dispose again (should not panic)
	devCtx.Dispose()
}

func TestDevContextDisposeNilFields(t *testing.T) {
	devCtx := &DevContext{}

	// Test dispose with nil dispatcher and datastore (should not panic)
	devCtx.Dispose()
}

func TestDistinguishGraphError(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}`,
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	// Test with namespace not found error
	nsErr := namespace.NewNamespaceNotFoundErr("unknown")
	devErr, wireErr := DistinguishGraphError(devCtx, nsErr, devinterface.DeveloperError_ASSERTION, 1, 2, "context")
	require.NoError(t, wireErr)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE, devErr.Kind)
	require.Equal(t, uint32(1), devErr.Line)
	require.Equal(t, uint32(2), devErr.Column)
	require.Equal(t, "context", devErr.Context)

	// Test with relation not found error
	relErr := namespace.NewRelationNotFoundErr("namespace", "unknown")
	devErr, wireErr = DistinguishGraphError(devCtx, relErr, devinterface.DeveloperError_ASSERTION, 3, 4, "context2")
	require.NoError(t, wireErr)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_UNKNOWN_RELATION, devErr.Kind)
	require.Equal(t, uint32(3), devErr.Line)
	require.Equal(t, uint32(4), devErr.Column)
	require.Equal(t, "context2", devErr.Context)

	// Test with max depth exceeded error
	maxDepthErr := dispatch.NewMaxDepthExceededError(nil)
	devErr, wireErr = DistinguishGraphError(devCtx, maxDepthErr, devinterface.DeveloperError_ASSERTION, 5, 6, "context3")
	require.NoError(t, wireErr)
	require.NotNil(t, devErr)
	require.Equal(t, devinterface.DeveloperError_MAXIMUM_RECURSION, devErr.Kind)
	require.Equal(t, uint32(5), devErr.Line)
	require.Equal(t, uint32(6), devErr.Column)
	require.Equal(t, "context3", devErr.Context)

	// Test with generic error (should return wire error)
	genericErr := errors.New("some generic error")
	devErr, wireErr = DistinguishGraphError(devCtx, genericErr, devinterface.DeveloperError_ASSERTION, 7, 8, "context4")
	require.Error(t, wireErr)
	require.Nil(t, devErr)
}

func TestRewriteACLError(t *testing.T) {
	ctx := t.Context()

	// Test with namespace not found error
	nsErr := namespace.NewNamespaceNotFoundErr("unknown")
	rewrittenErr := rewriteACLError(ctx, nsErr)
	require.Error(t, rewrittenErr)

	// Test with invalid revision error
	revErr := datastore.NewInvalidRevisionErr(datastore.NoRevision, datastore.RevisionStale)
	rewrittenErr = rewriteACLError(ctx, revErr)
	require.Error(t, rewrittenErr)
	st, ok := status.FromError(rewrittenErr)
	require.True(t, ok)
	require.Equal(t, codes.OutOfRange, st.Code())

	// Test with context deadline exceeded
	rewrittenErr = rewriteACLError(ctx, context.DeadlineExceeded)
	require.Error(t, rewrittenErr)
	st, ok = status.FromError(rewrittenErr)
	require.True(t, ok)
	require.Equal(t, codes.DeadlineExceeded, st.Code())

	// Test with context canceled
	rewrittenErr = rewriteACLError(ctx, context.Canceled)
	require.Error(t, rewrittenErr)
	st, ok = status.FromError(rewrittenErr)
	require.True(t, ok)
	require.Equal(t, codes.Canceled, st.Code())

	// Test with generic error
	genericErr := errors.New("generic error")
	rewrittenErr = rewriteACLError(ctx, genericErr)
	require.Equal(t, genericErr, rewrittenErr)
}

func TestNewDevContextWithCaveatError(t *testing.T) {
	_, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

caveat invalid_caveat(param unknown_type) {
	param == 42
}

definition document {
	relation viewer: user with invalid_caveat
}`,
	})

	require.NoError(t, err)
	require.NotNil(t, devErrs)
	require.Len(t, devErrs.InputErrors, 1)
	require.Equal(t, devinterface.DeveloperError_SCHEMA_ISSUE, devErrs.InputErrors[0].Kind)
}

func TestNewDevContextWithRelationshipValidationError(t *testing.T) {
	_, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}`,
		Relationships: []*core.RelationTuple{
			{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "document",
					ObjectId:  "doc1",
					Relation:  "viewer",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "nonexistent_user",
					ObjectId:  "user1",
					Relation:  "...",
				},
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, devErrs)
	require.Len(t, devErrs.InputErrors, 1)
	require.Equal(t, devinterface.DeveloperError_UNKNOWN_OBJECT_TYPE, devErrs.InputErrors[0].Kind)
}
