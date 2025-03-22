package common

import (
	"context"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

type fakeQuerier struct {
	queriesRun []string
}

func (fq *fakeQuerier) QueryFunc(ctx context.Context, f func(context.Context, Rows) error, sql string, args ...interface{}) error {
	fq.queriesRun = append(fq.queriesRun, sql)
	return nil
}

type fakeExplainable struct{}

func (fakeExplainable) BuildExplainQuery(sql string, args []any) (string, []any, error) {
	return "SOME EXPLAIN QUERY", nil, nil
}

func (fakeExplainable) ParseExplain(explain string) (datastore.ParsedExplain, error) {
	return datastore.ParsedExplain{}, nil
}

func (fakeExplainable) PreExplainStatements() []string {
	return []string{"SELECT SOMETHING"}
}

func TestRunExplainIfNecessaryWithoutEnabled(t *testing.T) {
	fq := &fakeQuerier{}

	err := runExplainIfNecessary(context.Background(), RelationshipsQueryBuilder{}, fq, fakeExplainable{})
	require.Nil(t, err)
	require.Nil(t, fq.queriesRun)
}

func TestRunExplainIfNecessaryWithEnabled(t *testing.T) {
	fq := &fakeQuerier{}

	schema := NewSchemaInformationWithOptions(
		WithRelationshipTableName("relationtuples"),
		WithColNamespace("ns"),
		WithColObjectID("object_id"),
		WithColRelation("relation"),
		WithColUsersetNamespace("subject_ns"),
		WithColUsersetObjectID("subject_object_id"),
		WithColUsersetRelation("subject_relation"),
		WithColCaveatName("caveat"),
		WithColCaveatContext("caveat_context"),
		WithColExpiration("expiration"),
		WithPlaceholderFormat(sq.Question),
		WithPaginationFilterType(TupleComparison),
		WithColumnOptimization(ColumnOptimizationOptionStaticValues),
		WithNowFunction("NOW"),
	)

	filterer := NewSchemaQueryFiltererForRelationshipsSelect(*schema, 100)
	filterer = filterer.FilterToResourceID("test")

	builder := RelationshipsQueryBuilder{
		Schema: *schema,
		SQLExplainCallbackForTest: func(ctx context.Context, sql string, args []any, shape queryshape.Shape, explain string, expectedIndexes options.SQLIndexInformation) error {
			return nil
		},
		filteringValues:  filterer.filteringColumnTracker,
		baseQueryBuilder: filterer,
	}

	err := runExplainIfNecessary(context.Background(), builder, fq, fakeExplainable{})
	require.Nil(t, err)
	require.Equal(t, fq.queriesRun, []string{"SELECT SOMETHING", "SOME EXPLAIN QUERY"})
}
