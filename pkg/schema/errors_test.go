package schema

import (
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestDefinitionNotFoundError(t *testing.T) {
	err := NewDefinitionNotFoundErr("document")

	var dnfe DefinitionNotFoundError
	require.ErrorAs(t, err, &dnfe)
	require.Equal(t, "document", dnfe.NotFoundNamespaceName())
	require.Contains(t, err.Error(), "document")

	require.Equal(t, map[string]string{"definition_name": "document"}, dnfe.DetailsMetadata())

	require.NotPanics(t, func() {
		dnfe.MarshalZerologObject(zerolog.Dict())
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

func TestCaveatNotFoundError(t *testing.T) {
	err := NewCaveatNotFoundErr("is_weekend")

	var cnfe CaveatNotFoundError
	require.ErrorAs(t, err, &cnfe)
	require.Equal(t, "is_weekend", cnfe.CaveatName())
	require.Contains(t, err.Error(), "is_weekend")

	require.Equal(t, map[string]string{"caveat_name": "is_weekend"}, cnfe.DetailsMetadata())

	require.NotPanics(t, func() {
		cnfe.MarshalZerologObject(zerolog.Dict())
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

func TestDuplicateAllowedRelationError(t *testing.T) {
	err := NewDuplicateAllowedRelationErr("document", "viewer", "user")

	var dare DuplicateAllowedRelationError
	require.ErrorAs(t, err, &dare)
	require.Contains(t, err.Error(), "duplicate")
	require.Contains(t, err.Error(), "user")
	require.Contains(t, err.Error(), "viewer")
	require.Contains(t, err.Error(), "document")

	require.Equal(t, map[string]string{
		"definition_name":  "document",
		"relation_name":    "viewer",
		"allowed_relation": "user",
	}, dare.DetailsMetadata())

	require.NotPanics(t, func() {
		dare.MarshalZerologObject(zerolog.Dict())
	})
}

func TestPermissionUsedOnLeftOfArrowError(t *testing.T) {
	err := NewPermissionUsedOnLeftOfArrowErr("document", "view", "edit")

	var pulae PermissionUsedOnLeftOfArrowError
	require.ErrorAs(t, err, &pulae)
	require.Contains(t, err.Error(), "view")
	require.Contains(t, err.Error(), "edit")
	require.Contains(t, err.Error(), "document")
	require.Contains(t, err.Error(), "left hand side of an arrow")

	require.Equal(t, map[string]string{
		"definition_name":      "document",
		"permission_name":      "view",
		"used_permission_name": "edit",
	}, pulae.DetailsMetadata())

	require.NotPanics(t, func() {
		pulae.MarshalZerologObject(zerolog.Dict())
	})
}

func TestWildcardUsedInArrowError(t *testing.T) {
	err := NewWildcardUsedInArrowErr("document", "view", "parent", "user", "member")

	var wuiae WildcardUsedInArrowError
	require.ErrorAs(t, err, &wuiae)
	require.Contains(t, err.Error(), "wildcard")

	require.Equal(t, map[string]string{
		"definition_name":        "document",
		"permission_name":        "view",
		"accessed_relation_name": "parent",
	}, wuiae.DetailsMetadata())

	require.NotPanics(t, func() {
		wuiae.MarshalZerologObject(zerolog.Dict())
	})
}

func TestMissingAllowedRelationsError(t *testing.T) {
	err := NewMissingAllowedRelationsErr("document", "viewer")

	var mare MissingAllowedRelationsError
	require.ErrorAs(t, err, &mare)
	require.Contains(t, err.Error(), "viewer")

	require.Equal(t, map[string]string{
		"definition_name": "document",
		"relation_name":   "viewer",
	}, mare.DetailsMetadata())

	require.NotPanics(t, func() {
		mare.MarshalZerologObject(zerolog.Dict())
	})
}

func TestTransitiveWildcardError(t *testing.T) {
	err := NewTransitiveWildcardErr("document", "viewer", "group", "member", "user", "owner")

	var twe TransitiveWildcardError
	require.ErrorAs(t, err, &twe)
	require.Contains(t, err.Error(), "wildcard")

	require.Equal(t, map[string]string{
		"definition_name": "document",
		"relation_name":   "viewer",
	}, twe.DetailsMetadata())

	require.NotPanics(t, func() {
		twe.MarshalZerologObject(zerolog.Dict())
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

func TestKnownEmptyFilterError(t *testing.T) {
	err := NewKnownEmptyFilterErr("no matching relations")

	var kefe KnownEmptyFilterError
	require.ErrorAs(t, err, &kefe)
	require.Equal(t, "no matching relations", kefe.WarningMessage())
	require.Contains(t, err.Error(), "no matching relations")

	require.Equal(t, map[string]string{
		"warning_message": "no matching relations",
	}, kefe.DetailsMetadata())

	require.NotPanics(t, func() {
		kefe.MarshalZerologObject(zerolog.Dict())
	})

	// KnownEmptyFilterError is wrapped in a TypeError.
	var te TypeError
	require.ErrorAs(t, err, &te)
}

func TestTypeErrorUnwrap(t *testing.T) {
	base := errors.New("underlying")
	te := TypeError{error: base}

	require.Equal(t, base, te.Unwrap())
	require.ErrorIs(t, te, base)
}

func TestAsTypeErrorNil(t *testing.T) {
	require.NoError(t, asTypeError(nil))
}

func TestAsTypeErrorDoubleWrap(t *testing.T) {
	inner := errors.New("inner")
	once := asTypeError(inner)
	twice := asTypeError(once)

	// Wrapping an already-TypeError should return the same error, not double-wrap.
	require.Equal(t, once, twice)
}

func TestNewTypeWithSourceErrorNilPosition(t *testing.T) {
	// A core.Relation has no source position set when zero-valued.
	withSource := &core.Relation{}
	require.Nil(t, withSource.GetSourcePosition())

	err := NewTypeWithSourceError(errors.New("bad"), withSource, "relation foo")
	require.Error(t, err)

	// The result should be wrapped as a TypeError.
	var te TypeError
	require.ErrorAs(t, err, &te)
}

func TestNewTypeWithSourceErrorWithPosition(t *testing.T) {
	withSource := &core.Relation{
		SourcePosition: &core.SourcePosition{
			ZeroIndexedLineNumber:     4,
			ZeroIndexedColumnPosition: 7,
		},
	}

	err := NewTypeWithSourceError(errors.New("bad"), withSource, "relation foo")
	require.Error(t, err)

	var te TypeError
	require.ErrorAs(t, err, &te)
}

func TestBacktickNames(t *testing.T) {
	require.Empty(t, backtickNames(nil))
	require.Equal(t, []string{"`a`"}, backtickNames([]string{"a"}))
	require.Equal(t, []string{"`a`", "`b`", "`c`"}, backtickNames([]string{"a", "b", "c"}))
}
