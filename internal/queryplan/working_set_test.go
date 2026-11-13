package queryplan

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	schema "github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestSchemaCacheRetainsMixedExclusionWorkingSet(t *testing.T) {
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("mixed-exclusion"),
		SchemaString: `caveat enabled(ok bool) { ok }
definition user {}
definition group {
 relation member: user
 relation owner: user
 permission access = member + owner
}
definition team {
 relation participant: user
}
definition document {
 relation viewer: user | group#member | group#access | team#participant
 relation banned: user with enabled
 permission view = viewer
 permission safe = view - banned
}`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)
	definitions := map[string]*core.NamespaceDefinition{}

	for _, definition := range compiled.ObjectDefinitions {
		definitions[definition.Name] = definition
	}

	stored := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				NamespaceDefinitions: definitions,
			},
		},
	})
	reader := schemaReader{stored: stored}
	converted, err := schema.BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	key := checkKey(t, 50)
	key.Permission = "safe"
	params := queryopt.RequestParams{Operation: query.OperationCheck, SubjectType: "user", SubjectRelation: "..."}
	var rootBuilds atomic.Int32
	buildRoot := func(context.Context) (query.CanonicalOutline, error) {
		rootBuilds.Add(1)
		outline, err := query.BuildOutlineFromSchema(converted, "document", "safe", key.Profile.BuildOptions()...)
		if err != nil {
			return query.CanonicalOutline{}, err
		}

		optimized, err := queryopt.ApplyOptimizations(outline, key.Profile.StructuralOptimizers(params), params)
		if err != nil {
			return query.CanonicalOutline{}, err
		}

		return dispatch.ApplyDispatchWrap(optimized, params)
	}
	root, _, err := Load(t.Context(), reader, key, buildRoot)
	require.NoError(t, err)

	// Admit the original wire subplans a real nested Check sends to its receivers.
	// A warm root must survive its own receiver working set, without weakening the
	// byte limit or treating a freshly recompiled tree as a cache hit.
	seen := map[string]bool{}
	_, err = query.Walk(root, func(node query.Iterator) (query.Iterator, error) {
		if _, boundary := node.(*dispatch.DispatchIterator); !boundary {
			return node, nil
		}

		var wire bytes.Buffer
		require.NoError(t, node.Serialize(&wire))

		if seen[wire.String()] {
			return node, nil
		}

		seen[wire.String()] = true
		receiverKey := Key{Profile: key.Profile, Operation: query.OperationCheck, SerializedPlan: wire.String()}
		_, _, err := Load(t.Context(), reader, receiverKey, func(context.Context) (query.CanonicalOutline, error) {
			outline, err := query.Decompile(node)
			if err != nil {
				return query.CanonicalOutline{}, err
			}

			keys := map[query.OutlineNodeID]query.CanonicalKey{}
			outline = query.FillMissingNodeIDs(outline, keys)
			var retain func(query.Outline, query.Iterator)
			retain = func(outline query.Outline, iterator query.Iterator) {
				keys[outline.ID] = iterator.CanonicalKey()

				for index, child := range iterator.Subiterators() {
					retain(outline.SubOutlines[index], child)
				}
			}
			retain(outline, node)
			return queryopt.ApplyOptimizations(
				query.CanonicalOutline{Root: outline, CanonicalKeys: keys},
				key.Profile.StructuralOptimizers(params),
				params,
			)
		})
		require.NoError(t, err)
		return node, nil
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(seen), 6, "exercise the service and nested receiver plans")
	reused, _, err := Load(t.Context(), reader, key, buildRoot)
	require.NoError(t, err)
	require.Same(t, root, reused)
	require.EqualValues(t, 1, rootBuilds.Load())
	cache, err := datastore.LoadOrStoreDerived(stored, cacheKey, func() *planCache { panic("cache must already exist") })
	require.NoError(t, err)
	require.LessOrEqual(t, cache.used, stored.CompiledQueryPlanCacheBudget())
	t.Logf("working set: %d entries, %d reserved bytes used", len(cache.entries), cache.used)
}
