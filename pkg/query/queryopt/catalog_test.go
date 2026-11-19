package queryopt_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/aliaschaincollapse"
	"github.com/authzed/spicedb/pkg/query/queryopt/caveatpushdown"
	"github.com/authzed/spicedb/pkg/query/queryopt/reachabilitypruning"
	"github.com/authzed/spicedb/pkg/query/queryopt/setsimplification"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func TestCatalogDescriptorsCompile(t *testing.T) {
	for _, opt := range []queryopt.Optimizer{caveatpushdown.New(), setsimplification.New(), reachabilitypruning.New(), aliaschaincollapse.New()} {
		t.Run(opt.Name, func(t *testing.T) {
			registered, err := queryopt.GetOptimization(opt.Name)
			require.NoError(t, err)
			co, err := query.CanonicalizeOutline(query.Outline{Type: query.NullIteratorType})
			require.NoError(t, err)
			result, err := queryopt.ApplyOptimizations(co, []queryopt.Optimizer{registered}, queryopt.RequestParams{})
			require.NoError(t, err)
			_, err = result.Compile()
			require.NoError(t, err)
		})
	}
	_, err := queryopt.GetOptimization("not-an-optimization")
	require.Error(t, err)
	// Reusing an existing name must fail without replacing its descriptor.
	require.Panics(t, func() { queryopt.MustRegisterOptimization(queryopt.Optimizer{Name: "set-simplification"}) })
}

func TestRegisteredRewrites(t *testing.T) {
	leaf := query.Outline{Type: query.DatastoreIteratorType, Args: &query.IteratorArgs{Relation: schema.NewTestBaseRelation("document", "viewer", "user", "...")}}
	alias := func(name string, child query.Outline) query.Outline {
		return query.Outline{Type: query.AliasIteratorType, Args: &query.IteratorArgs{DefinitionName: "document", RelationName: name}, SubOutlines: []query.Outline{child}}
	}
	t.Run("alias-chain-collapse", func(t *testing.T) {
		opt, err := queryopt.GetOptimization("alias-chain-collapse")
		require.NoError(t, err)
		co, err := query.CanonicalizeOutline(alias("outer", alias("inner", leaf)))
		require.NoError(t, err)
		result, err := queryopt.ApplyOptimizations(co, []queryopt.Optimizer{opt}, queryopt.RequestParams{})
		require.NoError(t, err)
		require.Equal(t, "inner", result.Root.Args.RelationName)
		require.Equal(t, []string{"outer"}, result.Root.Args.AliasedAs)
		require.Equal(t, query.DatastoreIteratorType, result.Root.SubOutlines[0].Type)
		_, err = result.Compile()
		require.NoError(t, err)
	})
	t.Run("set-simplification", func(t *testing.T) {
		other := query.Outline{Type: query.DatastoreIteratorType, Args: &query.IteratorArgs{Relation: schema.NewTestBaseRelation("document", "editor", "user", "...")}}
		root := query.Outline{Type: query.UnionIteratorType, SubOutlines: []query.Outline{leaf, {Type: query.IntersectionIteratorType, SubOutlines: []query.Outline{leaf, other}}}}
		co, err := query.CanonicalizeOutline(root)
		require.NoError(t, err)
		opt, err := queryopt.GetOptimization("set-simplification")
		require.NoError(t, err)
		result, err := queryopt.ApplyOptimizations(co, []queryopt.Optimizer{opt}, queryopt.RequestParams{})
		require.NoError(t, err)
		require.Equal(t, query.DatastoreIteratorType, result.Root.Type)
		_, err = result.Compile()
		require.NoError(t, err)
	})
}

// Higher-priority normalization must precede simplification; request pruning
// and alias collapse may run in either order after those two passes.
func TestDefaultPassOrder(t *testing.T) {
	params := queryopt.RequestParams{Operation: query.OperationCheck, SubjectType: "user", SubjectRelation: "..."}
	opts := queryopt.OptimizersForRequest(params)
	var applied []string
	for i, opt := range opts {
		transformFactory := opt.NewTransform
		name := opt.Name
		opts[i].NewTransform = func(p queryopt.RequestParams) queryopt.OutlineTransform {
			transform := transformFactory(p)
			return func(root query.Outline) query.Outline {
				applied = append(applied, name)
				return transform(root)
			}
		}
	}
	co, err := query.CanonicalizeOutline(query.Outline{Type: query.NullIteratorType})
	require.NoError(t, err)
	_, err = queryopt.ApplyOptimizations(co, opts, params)
	require.NoError(t, err)
	require.Len(t, applied, 4)
	require.Equal(t, []string{"simple-caveat-pushdown", "set-simplification"}, applied[:2])
	require.ElementsMatch(t, []string{"reachability-pruning", "alias-chain-collapse"}, applied[2:])
}
