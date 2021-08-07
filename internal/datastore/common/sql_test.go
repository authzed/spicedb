package common

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

func TestSplitting(t *testing.T) {
	schema := SchemaInformation{
		ColNamespace:        "namespace",
		ColObjectID:         "object",
		ColRelation:         "relation",
		ColUsersetNamespace: "usersetnamespace",
		ColUsersetObjectID:  "usersetobject",
		ColUsersetRelation:  "usersetrelation",
	}

	// Create a query with a very low split point.
	query := TupleQuery{
		Conn:               nil,
		Schema:             schema,
		PrepareTransaction: nil,

		Query:                     sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Select().From("sometable"),
		SplitAtEstimatedQuerySize: 1, // byte
	}

	tplQuery := query.WithUsersets([]*v0.ObjectAndRelation{
		{
			Namespace: "foo",
			ObjectId:  "bar",
			Relation:  "Lorem ipsum dolor",
		},
		{
			Namespace: "foo",
			ObjectId:  "bar2",
			Relation:  "Lorem ipsum dolor",
		},
	})

	casted, ok := tplQuery.(combinedTupleQuery)
	require.True(t, ok)
	require.Equal(t, 2, len(casted.subQueries))
}
