package common

import (
	"context"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// fakeQuerier is a Querier that records the SQL of every query it is asked to run
// without executing the row-scanning closure, so tests can assert which statements
// were issued.
type fakeQuerier struct {
	queriesRun []string
}

func (fq *fakeQuerier) QueryFunc(ctx context.Context, f func(context.Context, Rows) error, sql string, args ...any) error {
	fq.queriesRun = append(fq.queriesRun, sql)
	return nil
}

// erroringRows simulates a pgx result set for a query that was aborted mid-flight
// (e.g. its context was canceled): Next() reports no rows, and the real error is
// only available via Err().
type erroringRows struct {
	err error
}

func (erroringRows) Next() bool             { return false }
func (erroringRows) Scan(dest ...any) error { return nil }
func (r erroringRows) Err() error           { return r.err }

// erroringQuerier invokes the row-scanning closure with an erroringRows, mirroring
// pgxcommon.QuerierFuncs.QueryFunc: it surfaces rows.Err() only if the closure itself
// did not already return an error.
type erroringQuerier struct {
	err error
}

func (q erroringQuerier) QueryFunc(ctx context.Context, f func(context.Context, Rows) error, sql string, args ...any) error {
	if err := f(ctx, erroringRows(q)); err != nil {
		return err
	}
	return q.err
}

func newExplainableMock(t *testing.T, explainQuery string, preExplainStmts []string) datastore.Explainable {
	t.Helper()

	m := mocks.NewMockExplainable(gomock.NewController(t))
	m.EXPECT().PreExplainStatements().Return(preExplainStmts).Times(1)
	m.EXPECT().BuildExplainQuery(gomock.Any(), gomock.Any()).Return(explainQuery, nil, nil).Times(1)
	return m
}

func explainEnabledBuilder(t *testing.T) RelationshipsQueryBuilder {
	t.Helper()

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

	return RelationshipsQueryBuilder{
		Schema: *schema,
		SQLExplainCallbackForTest: func(ctx context.Context, sql string, args []any, shape queryshape.Shape, explain string, expectedIndexes options.SQLIndexInformation) error {
			return nil
		},
		filteringValues:  filterer.filteringColumnTracker,
		baseQueryBuilder: filterer,
	}
}

func TestRunExplainIfNecessary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		builder     func(t *testing.T) RelationshipsQueryBuilder
		explainable func(t *testing.T) datastore.Explainable
		querier     Querier[Rows]
		validate    func(t *testing.T, querier Querier[Rows], err error)
	}{
		{
			name:    "disabled: no explain statements are run",
			builder: func(t *testing.T) RelationshipsQueryBuilder { return RelationshipsQueryBuilder{} },
			// The callback is nil, so the explainable is never consulted.
			explainable: func(t *testing.T) datastore.Explainable {
				return mocks.NewMockExplainable(gomock.NewController(t))
			},
			querier: &fakeQuerier{},
			validate: func(t *testing.T, querier Querier[Rows], err error) {
				require.NoError(t, err)
				require.Nil(t, querier.(*fakeQuerier).queriesRun)
			},
		},
		{
			name:    "enabled: pre-explain and explain queries are issued",
			builder: explainEnabledBuilder,
			explainable: func(t *testing.T) datastore.Explainable {
				return newExplainableMock(t, "SOME EXPLAIN QUERY", []string{"SELECT SOMETHING"})
			},
			querier: &fakeQuerier{},
			validate: func(t *testing.T, querier Querier[Rows], err error) {
				require.NoError(t, err)
				require.Equal(t, []string{"SELECT SOMETHING", "SOME EXPLAIN QUERY"}, querier.(*fakeQuerier).queriesRun)
			},
		},
		{
			// Regression test: when the EXPLAIN query is aborted mid-flight (e.g. its
			// context is canceled because a LookupResources pagination limit was
			// reached), pgx reports no rows and exposes the real error only via
			// rows.Err(). runExplainIfNecessary must surface that error rather than the
			// misleading "received empty explain", so upstream cancellation handling
			// recognizes it.
			name:    "aborted mid-flight: surfaces rows error instead of empty explain",
			builder: explainEnabledBuilder,
			explainable: func(t *testing.T) datastore.Explainable {
				return newExplainableMock(t, "EXPLAIN (FORMAT JSON) SELECT 1", nil)
			},
			querier: erroringQuerier{err: context.Canceled},
			validate: func(t *testing.T, _ Querier[Rows], err error) {
				require.Error(t, err)
				require.ErrorIs(t, err, context.Canceled,
					"expected the underlying rows error to be surfaced, got a masked error: %v", err)
				require.NotContains(t, err.Error(), "received empty explain",
					"the real error must not be masked as an empty explain")
			},
		},
		{
			name:    "no rows and no error: returns empty explain",
			builder: explainEnabledBuilder,
			explainable: func(t *testing.T) datastore.Explainable {
				return newExplainableMock(t, "EXPLAIN (FORMAT JSON) SELECT 1", nil)
			},
			querier: erroringQuerier{err: nil},
			validate: func(t *testing.T, _ Querier[Rows], err error) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "received empty explain")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runExplainIfNecessary(t.Context(), tc.builder(t), tc.querier, tc.explainable(t))
			tc.validate(t, tc.querier, err)
		})
	}
}

// twoRows simulates a result set with two rows still buffered and no error of its own.
type twoRows struct{ remaining int }

func (r *twoRows) Next() bool {
	if r.remaining == 0 {
		return false
	}
	r.remaining--
	return true
}
func (*twoRows) Scan(dest ...any) error { return nil }
func (*twoRows) Err() error             { return nil }

// failsAfterCallbackQuerier mirrors pgxcommon.QuerierFuncs.QueryFunc when the query fails
// (e.g. on statement_timeout) after the consumer has stopped reading: the closure returns
// nil, then draining the remaining rows fails and QueryFunc returns rows.Err().
type failsAfterCallbackQuerier struct{}

func (failsAfterCallbackQuerier) QueryFunc(ctx context.Context, f func(context.Context, Rows) error, sql string, args ...any) error {
	if err := f(ctx, &twoRows{remaining: 2}); err != nil {
		return err
	}
	return context.Canceled
}

// Regression test for https://github.com/authzed/spicedb/issues/3520: when the consumer
// stops ranging over the iterator (as ReadRelationships does when resp.Send fails) and
// QueryFunc then returns an error, the iterator must not call yield again, or Go panics
// with "range function continued iteration after function for loop body returned false".
func TestQueryRelationshipsDoesNotYieldAfterConsumerStops(t *testing.T) {
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
	filterer := NewSchemaQueryFiltererForRelationshipsSelect(*schema, 100).FilterToResourceType("document")
	builder := RelationshipsQueryBuilder{
		Schema:           *schema,
		filteringValues:  filterer.filteringColumnTracker,
		baseQueryBuilder: filterer,
	}

	iterator, err := QueryRelationships[Rows, map[string]any](t.Context(), builder, failsAfterCallbackQuerier{}, nil)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		for _, err := range iterator {
			require.NoError(t, err)
			break
		}
	})

	// A consumer that keeps reading still receives the error.
	var errs []error
	for _, err := range iterator {
		if err != nil {
			errs = append(errs, err)
		}
	}
	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], context.Canceled)
}
