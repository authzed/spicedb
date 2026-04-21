package namespace

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestNamespaceNotFoundError(t *testing.T) {
	err := NewNamespaceNotFoundErr("document")

	var nnfe NamespaceNotFoundError
	require.ErrorAs(t, err, &nnfe)
	require.Equal(t, "document", nnfe.NotFoundNamespaceName())
	require.Contains(t, err.Error(), "document")

	require.Equal(t, map[string]string{"definition_name": "document"}, nnfe.DetailsMetadata())

	require.NotPanics(t, func() {
		nnfe.MarshalZerologObject(zerolog.Dict())
	})
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

func TestDuplicateRelationError(t *testing.T) {
	err := NewDuplicateRelationError("document", "viewer")

	var dre DuplicateRelationError
	require.ErrorAs(t, err, &dre)
	require.Contains(t, err.Error(), "duplicate")
	require.Contains(t, err.Error(), "viewer")
	require.Contains(t, err.Error(), "document")

	require.Equal(t, map[string]string{
		"definition_name":             "document",
		"relation_or_permission_name": "viewer",
	}, dre.DetailsMetadata())

	require.NotPanics(t, func() {
		dre.MarshalZerologObject(zerolog.Dict())
	})
}

func TestPermissionsCycleError(t *testing.T) {
	err := NewPermissionsCycleErr("document", []string{"view", "edit", "admin"})

	var pce PermissionsCycleError
	require.ErrorAs(t, err, &pce)
	require.Contains(t, err.Error(), "cycle")
	require.Contains(t, err.Error(), "view")
	require.Contains(t, err.Error(), "edit")
	require.Contains(t, err.Error(), "admin")

	require.Equal(t, map[string]string{
		"definition_name":  "document",
		"permission_names": "view,edit,admin",
	}, pce.DetailsMetadata())

	require.NotPanics(t, func() {
		pce.MarshalZerologObject(zerolog.Dict())
	})
}

func TestUnusedCaveatParameterError(t *testing.T) {
	err := NewUnusedCaveatParameterErr("is_weekend", "day")

	var ucpe UnusedCaveatParameterError
	require.ErrorAs(t, err, &ucpe)
	require.Contains(t, err.Error(), "day")
	require.Contains(t, err.Error(), "is_weekend")

	require.Equal(t, map[string]string{
		"caveat_name":    "is_weekend",
		"parameter_name": "day",
	}, ucpe.DetailsMetadata())

	require.NotPanics(t, func() {
		ucpe.MarshalZerologObject(zerolog.Dict())
	})
}
