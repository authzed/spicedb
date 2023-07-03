package namespace

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestReachabilityGraph(t *testing.T) {
	testCases := []struct {
		name                                 string
		schema                               string
		resourceType                         *core.RelationReference
		subjectType                          *core.RelationReference
		expectedFullEntrypointRelations      []rrtStruct
		expectedOptimizedEntrypointRelations []rrtStruct
	}{
		{
			"single relation",
			`definition user {}

			definition document {
				relation viewer: user
			}`,
			rr("document", "viewer"),
			rr("user", "..."),
			[]rrtStruct{rrt("document", "viewer", true)},
			[]rrtStruct{rrt("document", "viewer", true)},
		},
		{
			"simple permission",
			`definition user {}

			definition document {
				relation viewer: user
				permission view = viewer + nil
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{rrt("document", "viewer", true)},
			[]rrtStruct{rrt("document", "viewer", true)},
		},
		{
			"permission with multiple relations",
			`definition user {}

			definition document {
				relation viewer: user
				relation editor: user
				relation owner: user
				permission view = viewer + editor + owner
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "editor", true),
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
			},
			[]rrtStruct{
				rrt("document", "editor", true),
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
			},
		},
		{
			"permission with multiple relations under intersection",
			`definition user {}

			definition document {
				relation viewer: user
				relation editor: user
				relation owner: user
				permission view = viewer & editor & owner
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "editor", true),
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
			},
			[]rrtStruct{
				rrt("document", "viewer", true),
			},
		},
		{
			"permission with multiple relations under exclusion",
			`definition user {}

			definition document {
				relation viewer: user
				relation editor: user
				relation owner: user
				permission view = viewer - editor - owner
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "editor", true),
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
			},
			[]rrtStruct{
				rrt("document", "viewer", true),
			},
		},
		{
			"permission with arrow",
			`definition user {}

			definition organization {
				relation admin: user
			}

			definition document {
				relation org: organization
				relation viewer: user
				relation owner: user
				permission view = viewer + owner + org->admin
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
			},
			[]rrtStruct{
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
			},
		},
		{
			"permission with multi-level arrows",
			`definition user {}

			definition organization {
				relation admin: user
			}

			definition container {
				relation parent: organization | container
				permission admin = parent->admin
			}

			definition document {
				relation container: container
				relation viewer: user
				relation owner: user
				permission view = viewer + owner + container->admin
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
			},
			[]rrtStruct{
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
			},
		},
		{
			"permission with multi-level arrows and container",
			`definition user {}

			definition organization {
				relation admin: user
			}

			definition container {
				relation parent: organization | container
				relation localadmin: user
				permission admin = parent->admin + localadmin
			}

			definition document {
				relation container: container
				relation viewer: user
				relation owner: user
				permission view = viewer + owner + container->admin
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("container", "localadmin", true),
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
			},
			[]rrtStruct{
				rrt("container", "localadmin", true),
				rrt("document", "owner", true),
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
			},
		},
		{
			"recursive relation",
			`definition user {}

			definition group {
				relation direct_member: group#member | user
				relation manager: group#member | user
				permission member = direct_member + manager
			}

			definition document {
				relation viewer: user | group#member
				permission view = viewer
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "viewer", true),
				rrt("group", "direct_member", true),
				rrt("group", "manager", true),
			},
			[]rrtStruct{
				rrt("document", "viewer", true),
				rrt("group", "direct_member", true),
				rrt("group", "manager", true),
			},
		},
		{
			"arrow under exclusion",
			`definition user {}

			definition organization {
				relation admin: user
				relation banned: user
			}

			definition document {
				relation org: organization
				relation viewer: user
				permission view = (viewer - org->banned) + org->admin
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
				rrt("organization", "banned", true),
			},
			[]rrtStruct{
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
			},
		},
		{
			"multiple relations with different subject types",
			`definition platform1user {}
			definition platform2user {}

			definition document {
				relation viewer: platform1user | platform2user
				relation editor: platform1user
				relation owner: platform2user

				permission view = viewer + editor + owner
			}`,
			rr("document", "view"),
			rr("platform1user", "..."),
			[]rrtStruct{
				rrt("document", "editor", true),
				rrt("document", "viewer", true),
			},
			[]rrtStruct{
				rrt("document", "editor", true),
				rrt("document", "viewer", true),
			},
		},
		{
			"optimized reachability",
			`definition user {}

			definition organization {
				relation admin: user
				relation banned: user
			}

			definition document {
				relation org: organization
				relation viewer: user
				relation anotherrel: user
				relation thirdrel: user
				relation fourthrel: user
				permission view = ((((viewer - org->banned) & org->admin) + anotherrel) - thirdrel) + fourthrel
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("document", "anotherrel", true),
				rrt("document", "fourthrel", true),
				rrt("document", "thirdrel", true),
				rrt("document", "viewer", true),
				rrt("organization", "admin", true),
				rrt("organization", "banned", true),
			},
			[]rrtStruct{
				rrt("document", "anotherrel", true),
				rrt("document", "fourthrel", true),
				rrt("document", "viewer", true),
			},
		},
		{
			"optimized reachability, within expression",
			`definition user {}

			definition organization {
				relation admin: user
				relation banned: user
			}

			definition document {
				relation org: organization
				relation viewer: user
				relation anotherrel: user
				relation thirdrel: user
				relation fourthrel: user
				permission view = ((((viewer - org->banned) & org->admin) + anotherrel) - thirdrel) + fourthrel
			}`,
			rr("document", "view"),
			rr("document", "viewer"),
			[]rrtStruct{
				rrt("document", "view", false),
			},
			[]rrtStruct{
				rrt("document", "view", false),
			},
		},
		{
			"optimized reachability, within expression 2",
			`definition user {}

			definition organization {
				relation admin: user
				relation banned: user
			}

			definition document {
				relation org: organization
				relation viewer: user
				relation anotherrel: user
				relation thirdrel: user
				relation fourthrel: user
				permission view = ((((viewer - org->banned) & org->admin) + anotherrel) - thirdrel) + fourthrel
			}`,
			rr("document", "view"),
			rr("organization", "admin"),
			[]rrtStruct{
				rrt("document", "view", false),
			},
			[]rrtStruct{},
		},
		{
			"intermediate reachability",
			`definition user {}

			definition organization {
				relation admin: user
			}

			definition container {
				relation parent: organization | container
				relation localadmin: user
				permission admin = parent->admin + localadmin
				permission anotherthing = localadmin
			}

			definition document {
				relation container: container
				relation viewer: user
				relation owner: user
				permission view = viewer + owner + container->admin + container->anotherthing
			}`,
			rr("document", "view"),
			rr("container", "localadmin"),
			[]rrtStruct{
				rrt("container", "admin", true),
				rrt("container", "anotherthing", true),
			},
			[]rrtStruct{
				rrt("container", "admin", true),
				rrt("container", "anotherthing", true),
			},
		},
		{
			"intermediate reachability with intersection",
			`definition user {}

			definition organization {
				relation admin: user
			}

			definition container {
				relation parent: organization | container
				relation localadmin: user
				relation another: user
				permission admin = parent->admin + localadmin
				permission anotherthing = localadmin & another
			}

			definition document {
				relation container: container
				permission view = container->admin + container->anotherthing
			}`,
			rr("document", "view"),
			rr("container", "localadmin"),
			[]rrtStruct{
				rrt("container", "admin", true),
				rrt("container", "anotherthing", false),
			},
			[]rrtStruct{
				rrt("container", "admin", true),
				rrt("container", "anotherthing", false),
			},
		},
		{
			"relation reused",
			`definition user {}

			definition document {
				relation viewer: user
				relation another: user
				permission view = viewer + viewer
			}`,
			rr("document", "view"),
			rr("document", "viewer"),
			[]rrtStruct{
				rrt("document", "view", true),
			},
			[]rrtStruct{
				rrt("document", "view", true),
			},
		},
		{
			"relation does not exist on one type of the arrow",
			`definition user {}

			definition team {}

			definition organization {
				relation viewer: user
			}

			definition document {
				relation parent: organization | team
				permission view = parent->viewer
			}`,
			rr("document", "view"),
			rr("user", "..."),
			[]rrtStruct{
				rrt("organization", "viewer", true),
			},
			[]rrtStruct{
				rrt("organization", "viewer", true),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := datastoremw.ContextWithDatastore(context.Background(), ds)

			empty := ""
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.schema,
			}, &empty)
			require.NoError(err)

			lastRevision, err := ds.HeadRevision(context.Background())
			require.NoError(err)

			var rts *ValidatedNamespaceTypeSystem
			for _, nsDef := range compiled.ObjectDefinitions {
				reader := ds.SnapshotReader(lastRevision)
				ts, err := NewNamespaceTypeSystem(nsDef,
					ResolverForDatastoreReader(reader).WithPredefinedElements(PredefinedElements{
						Namespaces: compiled.ObjectDefinitions,
						Caveats:    compiled.CaveatDefinitions,
					}))
				require.NoError(err)

				vts, terr := ts.Validate(ctx)
				require.NoError(terr)

				if nsDef.Name == tc.resourceType.Namespace {
					rts = vts
				}
			}
			require.NotNil(rts)

			foundEntrypoints, err := ReachabilityGraphFor(rts).AllEntrypointsForSubjectToResource(ctx, tc.subjectType, tc.resourceType)
			require.NoError(err)
			verifyEntrypoints(require, foundEntrypoints, tc.expectedFullEntrypointRelations)

			foundOptEntrypoints, err := ReachabilityGraphFor(rts).OptimizedEntrypointsForSubjectToResource(ctx, tc.subjectType, tc.resourceType)
			require.NoError(err)
			verifyEntrypoints(require, foundOptEntrypoints, tc.expectedOptimizedEntrypointRelations)
		})
	}
}

func verifyEntrypoints(require *require.Assertions, foundEntrypoints []ReachabilityEntrypoint, expectedEntrypoints []rrtStruct) {
	expectedEntrypointRelations := make([]string, 0, len(expectedEntrypoints))
	isDirectMap := map[string]bool{}
	for _, expected := range expectedEntrypoints {
		expectedEntrypointRelations = append(expectedEntrypointRelations, tuple.StringRR(expected.relationRef))
		isDirectMap[tuple.StringRR(expected.relationRef)] = expected.isDirect
	}

	foundRelations := make([]string, 0, len(foundEntrypoints))
	for _, entrypoint := range foundEntrypoints {
		foundRelations = append(foundRelations, tuple.StringRR(entrypoint.ContainingRelationOrPermission()))
		if isDirect, ok := isDirectMap[tuple.StringRR(entrypoint.ContainingRelationOrPermission())]; ok {
			require.Equal(isDirect, entrypoint.IsDirectResult(), "found mismatch for whether a direct result for entrypoint for %s", entrypoint.parentRelation.Relation)
		}
	}

	sort.Strings(expectedEntrypointRelations)
	sort.Strings(foundRelations)
	require.Equal(expectedEntrypointRelations, foundRelations)
}

type rrtStruct struct {
	relationRef *core.RelationReference
	isDirect    bool
}

func rr(namespace, relation string) *core.RelationReference {
	return ns.RelationReference(namespace, relation)
}

func rrt(namespace, relation string, isDirect bool) rrtStruct {
	return rrtStruct{ns.RelationReference(namespace, relation), isDirect}
}
