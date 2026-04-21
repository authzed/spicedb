package graph

import (
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestCheckFailureError(t *testing.T) {
	base := errors.New("boom")
	err := NewCheckFailureErr(base)

	var cfe CheckFailureError
	require.ErrorAs(t, err, &cfe)
	require.ErrorIs(t, err, base)
	require.Contains(t, err.Error(), "error performing check")
	require.Contains(t, err.Error(), "boom")
}

func TestExpansionFailureError(t *testing.T) {
	base := errors.New("boom")
	err := NewExpansionFailureErr(base)

	var efe ExpansionFailureError
	require.ErrorAs(t, err, &efe)
	require.ErrorIs(t, err, base)
	require.Contains(t, err.Error(), "error performing expand")
	require.Contains(t, err.Error(), "boom")
}

func TestAlwaysFailError(t *testing.T) {
	err := NewAlwaysFailErr()

	var afe AlwaysFailError
	require.ErrorAs(t, err, &afe)
	require.Equal(t, "always fail", err.Error())
}

func TestRelationNotFoundError(t *testing.T) {
	err := NewRelationNotFoundErr("document", "viewer")

	var rnfe RelationNotFoundError
	require.ErrorAs(t, err, &rnfe)
	require.Equal(t, "document", rnfe.NamespaceName())
	require.Equal(t, "viewer", rnfe.NotFoundRelationName())
	require.Contains(t, err.Error(), "viewer")
	require.Contains(t, err.Error(), "document")

	require.Equal(t, map[string]string{
		"definition_name":             "document",
		"relation_or_permission_name": "viewer",
	}, rnfe.DetailsMetadata())

	require.NotPanics(t, func() {
		rnfe.MarshalZerologObject(zerolog.Dict())
	})
}

func TestRelationMissingTypeInfoError(t *testing.T) {
	err := NewRelationMissingTypeInfoErr("document", "viewer")

	var rmte RelationMissingTypeInfoError
	require.ErrorAs(t, err, &rmte)
	require.Equal(t, "document", rmte.NamespaceName())
	require.Equal(t, "viewer", rmte.RelationName())
	require.Contains(t, err.Error(), "missing type information")

	require.Equal(t, map[string]string{
		"definition_name": "document",
		"relation_name":   "viewer",
	}, rmte.DetailsMetadata())

	require.NotPanics(t, func() {
		rmte.MarshalZerologObject(zerolog.Dict())
	})
}

func TestWildcardNotAllowedError(t *testing.T) {
	err := NewWildcardNotAllowedErr("wildcard not allowed here", "resource.subject")

	var wne WildcardNotAllowedError
	require.ErrorAs(t, err, &wne)
	require.Contains(t, err.Error(), "wildcard not allowed here")

	status := wne.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestUnimplementedError(t *testing.T) {
	base := errors.New("not yet")
	err := NewUnimplementedErr(base)

	var ue UnimplementedError
	require.ErrorAs(t, err, &ue)
	require.ErrorIs(t, err, base)
	require.Equal(t, "not yet", err.Error())
}

func TestInvalidCursorError(t *testing.T) {
	err := NewInvalidCursorErr(2, &dispatch.Cursor{DispatchVersion: 1})

	var ice InvalidCursorError
	require.ErrorAs(t, err, &ice)
	require.Contains(t, err.Error(), "cursor is no longer valid")

	status := ice.GRPCStatus()
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.NotEmpty(t, status.Details())
}

func TestCheckFailureErrorUnwrap(t *testing.T) {
	base := errors.New("underlying")
	err := NewCheckFailureErr(base)

	var cfe CheckFailureError
	require.ErrorAs(t, err, &cfe)
	require.NotNil(t, cfe.Unwrap())
	require.ErrorIs(t, cfe.Unwrap(), base)
}

func TestExpansionFailureErrorUnwrap(t *testing.T) {
	base := errors.New("underlying")
	err := NewExpansionFailureErr(base)

	var efe ExpansionFailureError
	require.ErrorAs(t, err, &efe)
	require.NotNil(t, efe.Unwrap())
	require.ErrorIs(t, efe.Unwrap(), base)
}

func TestUnimplementedErrorUnwrap(t *testing.T) {
	base := errors.New("underlying")
	err := NewUnimplementedErr(base)

	var ue UnimplementedError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, base, ue.Unwrap())
}
