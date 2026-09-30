package options

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/testutil"
)

// TestReturnsAllUnorderedCoversEveryField sets each field of QueryOptions on the zero value
// and checks the verdict of ReturnsAllUnordered.
func TestReturnsAllUnorderedCoversEveryField(t *testing.T) {
	const (
		// disqualifying fields make ReturnsAllUnordered false when they are not zero.
		disqualifying = "disqualifying"
		// irrelevant fields change the columns of each relationship or only label the query.
		irrelevant = "irrelevant"
	)
	roles := map[string]string{
		"Limit":                     disqualifying,
		"Sort":                      disqualifying,
		"After":                     disqualifying,
		"BeforeOrEqual":             disqualifying,
		"SkipCaveats":               irrelevant,
		"SkipExpiration":            irrelevant,
		"SQLCheckAssertionForTest":  disqualifying,
		"SQLExplainCallbackForTest": disqualifying,
		"QueryShape":                irrelevant,
		"UseTupleComparison":        disqualifying,
	}

	var baseline QueryOptions
	require.True(t, baseline.ReturnsAllUnordered())

	typ := reflect.TypeFor[QueryOptions]()
	for name := range roles {
		_, ok := typ.FieldByName(name)
		require.True(t, ok, "the roles table lists %s, which QueryOptions does not have", name)
	}
	for i := range typ.NumField() {
		field := typ.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			role, ok := roles[field.Name]
			require.True(t, ok, "QueryOptions.%s is new: decide whether it changes which relationships a query returns "+
				"or their order, update QueryOptions.ReturnsAllUnordered and the serveFromSet function "+
				"in internal/datastore/proxy/fullrelationcache, then add the field to this table", field.Name)

			set := baseline
			reflect.ValueOf(&set).Elem().Field(i).Set(testutil.NonZeroValue(t, field.Type))
			require.Equal(t, role != disqualifying, set.ReturnsAllUnordered(), "%s set to a non-zero value", field.Name)
		})
	}
}
