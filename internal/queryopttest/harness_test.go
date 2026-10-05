package queryopttest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

func testSchema(t *testing.T, text string) Case {
	t.Helper()
	compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("test"), SchemaString: text}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)
	s, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, compiled.CaveatDefinitions)
	require.NoError(t, err)
	return Case{Schema: s, CaveatDefinitions: compiled.CaveatDefinitions}
}

func testPath(id, relation string) *query.Path {
	return &query.Path{Resource: query.NewObject("document", "o_a"), Relation: "view", Subject: query.NewObject("user", id).WithRelation(relation)}
}

func TestCompareResults(t *testing.T) {
	a, b := testPath("o_a", "..."), testPath("o_b", "...")
	for _, op := range []query.Operation{query.OperationCheck, query.OperationIterResources, query.OperationIterSubjects} {
		require.Error(t, compareResults(context.Background(), nil, op, []*query.Path{a}, nil, nil))
		require.Error(t, compareResults(context.Background(), nil, op, nil, []*query.Path{a}, nil))
		require.NoError(t, compareResults(context.Background(), nil, op, []*query.Path{a}, []*query.Path{a, a}, nil))
	}
	require.NoError(t, compareResults(context.Background(), nil, query.OperationIterSubjects, []*query.Path{a, b}, []*query.Path{b, a}, nil))
	require.Error(t, compareResults(context.Background(), nil, query.OperationIterSubjects, []*query.Path{a}, []*query.Path{testPath("o_a", "member")}, nil))
	wild := testPath("*", "...")
	excluded := testPath("*", "...")
	excluded.ExcludedSubjects = []*query.Path{a}
	require.Error(t, compareResults(context.Background(), nil, query.OperationIterSubjects, []*query.Path{wild}, []*query.Path{excluded}, nil))
	// An explicit grant restores an excluded wildcard member.
	require.NoError(t, compareResults(context.Background(), nil, query.OperationIterSubjects, []*query.Path{wild}, []*query.Path{excluded, a}, nil))
}

func TestHarnessRejectsCorruptTransforms(t *testing.T) {
	s := testSchema(t, `definition user {}
 definition document {
 relation viewer: user
 permission view = viewer
 }`)
	drop := optimization.Optimizer{Name: "bad-drop", NewTransform: func(optimization.RequestParams) optimization.OutlineTransform {
		return func(query.Outline) query.Outline { return query.Outline{Type: query.NullIteratorType} }
	}}
	add := optimization.Optimizer{Name: "bad-add", NewTransform: func(optimization.RequestParams) optimization.OutlineTransform {
		return func(query.Outline) query.Outline {
			return query.Outline{Type: query.FixedIteratorType, Args: &query.IteratorArgs{FixedPaths: []query.Path{*testPath("o_a", "...")}}}
		}
	}}
	for _, op := range []query.Operation{query.OperationCheck, query.OperationIterSubjects, query.OperationIterResources} {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			c := s
			c.Relationships = []tuple.Relationship{tuple.MustParse("document:o_a#viewer@user:o_a")}
			c.Requests = []Request{{Params: optimization.RequestParams{Operation: op, SubjectType: "user", SubjectRelation: "..."}, Resource: query.NewObject("document", "o_a"), Permission: "view", Subject: query.NewObject("user", "o_a").WithEllipses()}}
			_, err := verifyCase(t.Context(), Config{Optimizer: drop}, c)
			require.ErrorContains(t, err, "semantic mismatch")
			c.Relationships = nil
			_, err = verifyCase(t.Context(), Config{Optimizer: add}, c)
			require.ErrorContains(t, err, "semantic mismatch")
		})
	}
}

func TestTargetedCaseMustExercisePass(t *testing.T) {
	c := testSchema(t, `definition user {}
 definition document {
 relation viewer: user
 permission view = viewer
 }`)
	c.Requests = Requests("document", "view", "user")
	c.RequireChange = true
	noop := optimization.Optimizer{Name: "noop", NewTransform: func(optimization.RequestParams) optimization.OutlineTransform {
		return func(o query.Outline) query.Outline { return o }
	}}
	_, err := verifyCase(t.Context(), Config{Optimizer: noop}, c)
	require.ErrorContains(t, err, "did not change")
	_, err = verifyCase(t.Context(), Config{Optimizer: noop, Applicable: func(optimization.RequestParams) bool { return false }}, c)
	require.ErrorContains(t, err, "no applicable requests")
}

func TestConditionalComparison(t *testing.T) {
	s := testSchema(t, `caveat enabled(ok bool) { ok } definition user {} definition document { relation viewer: user with enabled
 permission view = viewer
 }`)
	reader, closeReader, err := caseReader(t.Context(), s)
	require.NoError(t, err)
	defer closeReader()
	sr, err := reader.ReadSchema(t.Context())
	require.NoError(t, err)
	a := testPath("o_a", "...")
	a.Caveat = caveats.CaveatAsExpr(&core.ContextualizedCaveat{CaveatName: "enabled"})
	b := testPath("o_a", "...")
	b.Caveat = &core.CaveatExpression{OperationOrCaveat: &core.CaveatExpression_Operation{Operation: &core.CaveatOperation{Op: core.CaveatOperation_OR, Children: []*core.CaveatExpression{a.Caveat, a.Caveat}}}}
	for _, ctx := range []map[string]any{nil, {"ok": true}, {"ok": false}} {
		require.NoError(t, compareResults(t.Context(), sr, query.OperationCheck, []*query.Path{a}, []*query.Path{b}, ctx))
	}
	require.Error(t, compareResults(t.Context(), sr, query.OperationCheck, []*query.Path{a}, []*query.Path{testPath("o_a", "...")}, nil))
	// Removing a caveat is a semantic mutation even if the path still exists.
	c := Case{Schema: s.Schema, CaveatDefinitions: s.CaveatDefinitions, Relationships: []tuple.Relationship{tuple.MustParse("document:o_a#viewer@user:o_a[enabled]")}, Requests: []Request{{Params: optimization.RequestParams{Operation: query.OperationCheck, SubjectType: "user", SubjectRelation: "..."}, Resource: query.NewObject("document", "o_a"), Permission: "view", Subject: query.NewObject("user", "o_a").WithEllipses()}}}
	opt := optimization.Optimizer{Name: "bad-unconditional", NewTransform: func(optimization.RequestParams) optimization.OutlineTransform {
		return func(query.Outline) query.Outline {
			return query.Outline{Type: query.FixedIteratorType, Args: &query.IteratorArgs{FixedPaths: []query.Path{*testPath("o_a", "...")}}}
		}
	}}
	_, err = verifyCase(t.Context(), Config{Optimizer: opt}, c)
	require.ErrorContains(t, err, "semantic mismatch")
}

func TestConditionalWildcardExclusions(t *testing.T) {
	c := testSchema(t, `caveat enabled(ok bool) { ok }
 definition user {}
 definition document {
 relation viewer: user with enabled
 }`)
	reader, closeReader, err := caseReader(t.Context(), c)
	require.NoError(t, err)
	defer closeReader()
	sr, err := reader.ReadSchema(t.Context())
	require.NoError(t, err)
	expr := caveats.CaveatAsExpr(&core.ContextualizedCaveat{CaveatName: "enabled"})
	excluded := testPath("o_a", "...")
	excluded.Caveat = expr
	wild := testPath("*", "...")
	wild.ExcludedSubjects = []*query.Path{excluded}
	base := testPath("*", "...")
	base.ExcludedSubjects = []*query.Path{testPath("o_a", "...")}
	grant := testPath("o_a", "...")
	grant.Caveat = caveats.Invert(expr)
	for _, values := range []map[string]any{nil, {"ok": true}, {"ok": false}} {
		require.NoError(t, compareResults(t.Context(), sr, query.OperationIterSubjects, []*query.Path{wild}, []*query.Path{base, grant}, values))
	}
	badExcluded := testPath("o_a", "...")
	badExcluded.Caveat = caveats.Invert(expr)
	badWild := testPath("*", "...")
	badWild.ExcludedSubjects = []*query.Path{badExcluded}
	for _, values := range []map[string]any{{"ok": true}, {"ok": false}} {
		require.Error(t, compareResults(t.Context(), sr, query.OperationIterSubjects, []*query.Path{wild}, []*query.Path{badWild}, values))
	}
	require.Error(t, compareResults(t.Context(), sr, query.OperationIterSubjects, []*query.Path{wild}, []*query.Path{testPath("*", "...")}, nil))
}

// Compare iterator sets before Context.IterSubjects' API projection drops
// wildcards; otherwise wildcard properties silently test empty results.
func TestExecutePreservesWildcardSemantics(t *testing.T) {
	c := testSchema(t, `definition user {}
 definition document {
 relation viewer: user:*
 permission view = viewer
 }`)
	c.Relationships = []tuple.Relationship{tuple.MustParse("document:o_a#viewer@user:*")}
	req := Requests("document", "view", "user")[1]
	reader, closeReader, err := caseReader(t.Context(), c)
	require.NoError(t, err)
	defer closeReader()
	plan, err := query.BuildOutlineFromSchema(c.Schema, "document", "view")
	require.NoError(t, err)
	paths, err := execute(t.Context(), reader, plan, req, nil)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Equal(t, "*", paths[0].Subject.ObjectID)
}

func TestCheckRunsGeneratedAndTargetedCases(t *testing.T) {
	noop := optimization.Optimizer{Name: "noop", NewTransform: func(optimization.RequestParams) optimization.OutlineTransform {
		return func(o query.Outline) query.Outline { return o }
	}}
	setup := false
	Check(t, Config{
		Optimizer: noop,
		Setup:     func(testing.TB) { setup = true },
		Cases: map[string]func(*rapid.T) Case{
			"targeted": func(t *rapid.T) Case {
				c := ParseCase(t, `definition user {}
 definition document {
 relation viewer: user
 permission view = viewer
 }`)
				c.Relationships = Relationships(t, "document:o_a#viewer@user:o_a", "document:o_b#viewer@user:o_b")
				c.Requests = Requests("document", "view", "user")
				return c
			},
		},
	})
	require.True(t, setup)
}

func TestSchemaDescription(t *testing.T) {
	require.Nil(t, schemaDescription(nil))
	c := testSchema(t, `definition user {}`)
	require.Contains(t, schemaDescription(c.Schema), "definitions=")
}

func TestVerifyCaseRejectsInvalidCases(t *testing.T) {
	noop := optimization.Optimizer{Name: "noop", NewTransform: func(optimization.RequestParams) optimization.OutlineTransform {
		return func(o query.Outline) query.Outline { return o }
	}}
	_, err := verifyCase(t.Context(), Config{Optimizer: noop}, Case{})
	require.ErrorContains(t, err, "nil case schema")

	caveated := testSchema(t, `caveat enabled(ok bool) { ok }
 definition user {}
 definition document {
 relation viewer: user with enabled
 permission view = viewer
 }`)
	_, err = verifyCase(t.Context(), Config{Optimizer: noop}, Case{Schema: caveated.Schema, Requests: Requests("document", "view", "user")})
	require.ErrorContains(t, err, "use ParseCase")

	c := caveated
	c.Relationships = []tuple.Relationship{tuple.MustParse("document:o_a#viewer@user:o_a[missing]")}
	_, err = verifyCase(t.Context(), Config{Optimizer: noop}, c)
	require.Error(t, err)

	c = caveated
	c.Requests = Requests("document", "view", "user")
	c.Requests[0].Params.SubjectType = "document"
	_, err = verifyCase(t.Context(), Config{Optimizer: noop}, c)
	require.ErrorContains(t, err, "do not match subject")

	c.Requests = Requests("document", "missing", "user")
	_, err = verifyCase(t.Context(), Config{Optimizer: noop}, c)
	require.Error(t, err)

	// A caveat context with the wrong parameter type fails during execution.
	c.Relationships = []tuple.Relationship{tuple.MustParse("document:o_a#viewer@user:o_a[enabled]")}
	c.Requests = Requests("document", "view", "user")[:1]
	c.Requests[0].CaveatContexts = []map[string]any{{"ok": "not-a-bool"}}
	_, err = verifyCase(t.Context(), Config{Optimizer: noop}, c)
	require.ErrorContains(t, err, "before execution")

	c.Requests = Requests("document", "view", "user")[:1]
	c.Requests[0].Params.Operation = query.Operation(255)
	_, err = verifyCase(t.Context(), Config{Optimizer: noop}, c)
	require.ErrorContains(t, err, "unsupported operation")
}

func TestCompareResultsEvaluationErrors(t *testing.T) {
	c := testSchema(t, `caveat enabled(ok bool) { ok }
 definition user {}
 definition document {
 relation viewer: user with enabled
 }`)
	reader, closeReader, err := caseReader(t.Context(), c)
	require.NoError(t, err)
	defer closeReader()
	sr, err := reader.ReadSchema(t.Context())
	require.NoError(t, err)
	unknown := testPath("o_a", "...")
	unknown.Caveat = caveats.CaveatAsExpr(&core.ContextualizedCaveat{CaveatName: "unknown"})
	require.Error(t, compareResults(t.Context(), sr, query.OperationCheck, []*query.Path{unknown}, nil, nil))
	require.Error(t, compareResults(t.Context(), sr, query.OperationCheck, nil, []*query.Path{unknown}, nil))

	// An unconditional path absorbs a conditional one for the same key.
	conditional := testPath("o_a", "...")
	conditional.Caveat = caveats.CaveatAsExpr(&core.ContextualizedCaveat{CaveatName: "enabled"})
	plain := testPath("o_a", "...")
	require.NoError(t, compareResults(t.Context(), sr, query.OperationCheck, []*query.Path{conditional, plain}, []*query.Path{plain}, map[string]any{"ok": false}))
	require.NoError(t, compareResults(t.Context(), sr, query.OperationCheck, []*query.Path{conditional, conditional}, []*query.Path{conditional}, map[string]any{"ok": true}))
}
