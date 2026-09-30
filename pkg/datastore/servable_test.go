package datastore

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/testutil"
)

// TestIsServableFromCompleteSetCoversEveryField sets each field of RelationshipsFilter on a servable baseline
// and checks the verdict of IsServableFromCompleteSet.
func TestIsServableFromCompleteSetCoversEveryField(t *testing.T) {
	const (
		// required fields make the filter non-servable when they are zero.
		required = "required"
		// disqualifying fields make the filter non-servable when they are not zero.
		disqualifying = "disqualifying"
		// irrelevant fields are predicates that a caller applies to each relationship of a complete set.
		irrelevant = "irrelevant"
	)
	roles := map[string]string{
		"OptionalResourceType":      required,
		"OptionalResourceIds":       required,
		"OptionalResourceIDPrefix":  disqualifying,
		"OptionalResourceRelation":  required,
		"OptionalSubjectsSelectors": irrelevant,
		"OptionalCaveatNameFilter":  irrelevant,
		"OptionalExpirationOption":  irrelevant,
	}

	baseline := RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"foo"},
		OptionalResourceRelation: "viewer",
	}
	require.True(t, baseline.IsServableFromCompleteSet())

	typ := reflect.TypeFor[RelationshipsFilter]()
	for name := range roles {
		_, ok := typ.FieldByName(name)
		require.True(t, ok, "the roles table lists %s, which RelationshipsFilter does not have", name)
	}
	for i := range typ.NumField() {
		field := typ.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			role, ok := roles[field.Name]
			require.True(t, ok, "RelationshipsFilter.%s is new: decide whether a complete set can answer a filter "+
				"that sets it, update RelationshipsFilter.IsServableFromCompleteSet and the serveFromSet function "+
				"in internal/datastore/proxy/fullrelationcache, then add the field to this table", field.Name)

			zeroed := baseline
			reflect.ValueOf(&zeroed).Elem().Field(i).SetZero()
			require.Equal(t, role != required, zeroed.IsServableFromCompleteSet(), "%s set to zero", field.Name)

			set := baseline
			reflect.ValueOf(&set).Elem().Field(i).Set(testutil.NonZeroValue(t, field.Type))
			require.Equal(t, role != disqualifying, set.IsServableFromCompleteSet(), "%s set to a non-zero value", field.Name)
		})
	}
}
