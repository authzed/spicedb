package compiler_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/decorators"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func compileWithTestDecorators(t *testing.T, schema string) (*compiler.CompiledSchema, error) {
	t.Helper()
	return compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType(),
		compiler.WithDecoratorRegistry(decorators.TestRegistry))
}

func TestCompileDecoratorOnDefinition(t *testing.T) {
	t.Parallel()

	compiled, err := compileWithTestDecorators(t, `use testdecorators

@testall(needed: 7, label: "hi", on: true, mode: hash)
definition user {}`)
	require.NoError(t, err)

	def := compiled.ObjectDefinitions[0]
	require.Len(t, def.GetDecorators(), 1)

	d := def.GetDecorators()[0]
	require.Equal(t, "testall", d.GetName())
	require.Equal(t, decorators.TestFlag, d.GetRequiredFlag())
	require.Len(t, d.GetParameters(), 4)
	require.Equal(t, int64(7), d.GetParameters()[0].GetIntValue())
	require.Equal(t, "hi", d.GetParameters()[1].GetStringValue())
	require.True(t, d.GetParameters()[2].GetBoolValue())
	require.Equal(t, "hash", d.GetParameters()[3].GetEnumValue())
}

func TestCompileDecoratorOnRelationAndSubjectType(t *testing.T) {
	t.Parallel()

	compiled, err := compileWithTestDecorators(t, `use testdecorators

definition user {}

definition document {
	@testrel
	relation viewer: @testsub user

	@testrel
	permission view = viewer
}`)
	require.NoError(t, err)

	doc := compiled.ObjectDefinitions[1]
	viewer := doc.GetRelation()[0]
	require.Equal(t, "viewer", viewer.GetName())
	require.Len(t, viewer.GetDecorators(), 1)
	require.Equal(t, "testrel", viewer.GetDecorators()[0].GetName())

	allowed := viewer.GetTypeInformation().GetAllowedDirectRelations()[0]
	require.Len(t, allowed.GetDecorators(), 1)
	require.Equal(t, "testsub", allowed.GetDecorators()[0].GetName())

	view := doc.GetRelation()[1]
	require.Len(t, view.GetDecorators(), 1)
}

func TestCompileDecoratorOnCaveat(t *testing.T) {
	t.Parallel()

	compiled, err := compileWithTestDecorators(t, `use testdecorators

@testcaveat
caveat somecaveat(someparam int) {
	someparam == 42
}`)
	require.NoError(t, err)
	require.Len(t, compiled.CaveatDefinitions[0].GetDecorators(), 1)
}

func TestCompileDecoratorOnPartialAppliesToIncluders(t *testing.T) {
	t.Parallel()

	compiled, err := compileWithTestDecorators(t, `use testdecorators
use partial

@testdef
partial base {
	relation viewer: user
}

definition user {}

definition document {
	...base
}`)
	require.NoError(t, err)

	doc := compiled.ObjectDefinitions[1]
	require.Equal(t, "document", doc.GetName())
	require.Len(t, doc.GetDecorators(), 1)
	require.Equal(t, "testdef", doc.GetDecorators()[0].GetName())
}

func TestCompileDecoratorErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		schema      string
		expectedErr string
	}{
		{
			name:        "unknown decorator",
			schema:      "use testdecorators\n\n@nope\ndefinition user {}",
			expectedErr: "unknown decorator `@nope`",
		},
		{
			name:        "missing use flag",
			schema:      "@testdef\ndefinition user {}",
			expectedErr: "decorator `@testdef` requires `use testdecorators`",
		},
		{
			name:        "wrong site",
			schema:      "use testdecorators\n\ndefinition user {}\ndefinition document {\n\t@testdef\n\trelation viewer: user\n}",
			expectedErr: "decorator `@testdef` is not permitted on a relation",
		},
		{
			// Pins the SiteRelation/SitePermission distinction: `@testdef` is definition-only,
			// so it must be rejected on a permission just as it is on a relation, above. If the
			// translateDecorators call were ever hoisted into the shared
			// translateRelationOrPermission dispatcher with a single hardcoded site, one of
			// these two cases would start asserting the wrong error message.
			name:        "wrong site permission",
			schema:      "use testdecorators\n\ndefinition user {}\ndefinition document {\n\trelation viewer: user\n\n\t@testdef\n\tpermission view = viewer\n}",
			expectedErr: "decorator `@testdef` is not permitted on a permission",
		},
		{
			name:        "missing required parameter",
			schema:      "use testdecorators\n\n@testall\ndefinition user {}",
			expectedErr: "missing required parameter `needed` for decorator `@testall`",
		},
		{
			name:        "bad enum value",
			schema:      "use testdecorators\n\n@testall(needed: 1, mode: nope)\ndefinition user {}",
			expectedErr: "invalid value `nope` for parameter `mode`",
		},
		{
			name:        "duplicate decorator",
			schema:      "use testdecorators\n\n@testdef\n@testdef\ndefinition user {}",
			expectedErr: "decorator `@testdef` specified more than once",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := compileWithTestDecorators(t, test.schema)
			require.ErrorContains(t, err, test.expectedErr)
		})
	}
}

func TestCompileDecoratorRejectedByDefaultRegistry(t *testing.T) {
	t.Parallel()

	// The production registry ships empty, so any decorator is unknown.
	_, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: "use testdecorators\n\n@testdef\ndefinition user {}",
	}, compiler.AllowUnprefixedObjectType())
	require.ErrorContains(t, err, "unknown decorator `@testdef`")
}

// TestCompileDecoratorNestedPartialPropagation covers behavior beyond the base brief: a
// decorator declared on a partial must reach a definition even when the partial is
// included transitively, through another partial, rather than directly. `derived` is
// declared (and thus translated) after `base`, so this exercises the ordinary,
// already-resolved lookup path in translatePartialReference/translateRelationsAndPermissions
// rather than the unresolvedPartials retry path (see the sibling out-of-order test below
// for that path).
func TestCompileDecoratorNestedPartialPropagation(t *testing.T) {
	t.Parallel()

	compiled, err := compileWithTestDecorators(t, `use testdecorators
use partial

@testdef
partial base {
	relation viewer: user
}

partial derived {
	...base
}

definition user {}

definition document {
	...derived
}`)
	require.NoError(t, err)

	doc := compiled.ObjectDefinitions[1]
	require.Equal(t, "document", doc.GetName())
	require.Len(t, doc.GetDecorators(), 1)
	require.Equal(t, "testdef", doc.GetDecorators()[0].GetName())
}

// TestCompileDecoratorNestedPartialPropagationOutOfOrder is identical in effect to
// TestCompileDecoratorNestedPartialPropagation above, except `derived` is declared BEFORE
// the `base` partial it references. collectPartials translates partials in declaration
// order, so translatePartial(derived) runs first, finds `base` not yet in
// tctx.compiledPartials, and defers `derived` onto tctx.unresolvedPartials keyed by
// "base". Only once translatePartial(base) later succeeds does the deferred retry for
// `derived` run (translatePartial's "waitingPartials" loop). This test pins that the
// retried translation of `derived` still merges in `base`'s decorator, not just its
// relations.
func TestCompileDecoratorNestedPartialPropagationOutOfOrder(t *testing.T) {
	t.Parallel()

	compiled, err := compileWithTestDecorators(t, `use testdecorators
use partial

partial derived {
	...base
}

@testdef
partial base {
	relation viewer: user
}

definition user {}

definition document {
	...derived
}`)
	require.NoError(t, err)

	doc := compiled.ObjectDefinitions[1]
	require.Equal(t, "document", doc.GetName())
	require.Len(t, doc.GetDecorators(), 1)
	require.Equal(t, "testdef", doc.GetDecorators()[0].GetName())
}

// TestCompileDecoratorIdenticalDuplicateAcrossPartialsCollapses covers mergeDecorators'
// non-conflicting branch: two different partials, included by the same definition, each
// apply the identical (parameterless) decorator. The result must collapse to a single
// decorator rather than erroring or duplicating.
func TestCompileDecoratorIdenticalDuplicateAcrossPartialsCollapses(t *testing.T) {
	t.Parallel()

	compiled, err := compileWithTestDecorators(t, `use testdecorators
use partial

@testdef
partial base1 {
	relation viewer: user
}

@testdef
partial base2 {
	relation editor: user
}

definition user {}

definition document {
	...base1
	...base2
}`)
	require.NoError(t, err)

	doc := compiled.ObjectDefinitions[1]
	require.Len(t, doc.GetDecorators(), 1,
		"identical decorators contributed by two different partials must collapse to one")
}

// TestCompileDecoratorConflictingParametersAcrossPartials covers mergeDecorators' error
// branch (decorators.go), which was previously unreachable by any committed test: two
// partials apply the same decorator name with different parameters, and both are included
// by the same definition. This must be rejected, and the resulting error must carry a
// source position like every other compiler error, not just a bare message.
func TestCompileDecoratorConflictingParametersAcrossPartials(t *testing.T) {
	t.Parallel()

	_, err := compileWithTestDecorators(t, `use testdecorators
use partial

@testall(needed: 1)
partial base1 {
	relation viewer: user
}

@testall(needed: 2)
partial base2 {
	relation editor: user
}

definition user {}

definition document {
	...base1
	...base2
}`)
	require.Error(t, err)

	var contextErr compiler.WithContextError
	require.ErrorAs(t, err, &contextErr)
	require.Equal(t,
		"parse error in `test`, line 18, column 2: decorator `@testall` is applied with conflicting parameters",
		contextErr.Error())
}
