package namespace

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestTypeSystem(t *testing.T) {
	testCases := []struct {
		name            string
		toCheck         *pb.NamespaceDefinition
		otherNamespaces []*pb.NamespaceDefinition
		expectedError   string
	}{
		{
			"invalid relation in computed_userset",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editors"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*pb.NamespaceDefinition{},
			"In computed_userset for relation `viewer`: relation `editors` not found",
		},
		{
			"invalid relation in tuple_to_userset",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parents", "viewer"),
				)),
			),
			[]*pb.NamespaceDefinition{},
			"In tuple_to_userset for relation `viewer`: relation `parents` not found",
		},
		{
			"rewrite without this and types",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.RelationReference("document", "owner")),
			),
			[]*pb.NamespaceDefinition{},
			"No direct relations are allowed under relation `editor`",
		},
		{
			"relation in relation types has invalid namespace",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.RelationReference("someinvalidns", "...")),
				ns.Relation("editor", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*pb.NamespaceDefinition{},
			"Could not lookup namespace `someinvalidns` for relation `owner`: invalid namespace",
		},
		{
			"relation in relation types has invalid relation",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.RelationReference("anotherns", "foobar")),
				ns.Relation("editor", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace(
					"anotherns",
				),
			},
			"For relation `owner`: relation `foobar` not found under namespace `anotherns`",
		},
		{
			"full type check",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.RelationReference("user", "...")),
				ns.Relation("can_comment",
					nil,
					ns.RelationReference("user", "..."),
					ns.RelationReference("folder", "can_comment"),
				),
				ns.Relation("editor",
					ns.Union(
						ns.ComputedUserset("owner"),
						ns.This(),
					),
					ns.RelationReference("user", "..."),
				),
				ns.Relation("parent", nil, ns.RelationReference("folder", "...")),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				), ns.RelationReference("user", "...")),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"folder",
					ns.Relation("can_comment", nil, ns.RelationReference("user", "...")),
				),
			},
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)

			nsm, err := NewCachingNamespaceManager(ds, 0*time.Second, nil)
			require.NoError(err)

			for _, otherNS := range tc.otherNamespaces {
				_, err := ds.WriteNamespace(context.Background(), otherNS)
				require.NoError(err)
			}

			ts, err := BuildNamespaceTypeSystem(tc.toCheck, nsm)
			require.NoError(err)

			terr := ts.Validate(context.Background())
			if tc.expectedError == "" {
				require.NoError(terr)
			} else {
				require.Error(terr)
				require.Equal(tc.expectedError, terr.Error())
			}
		})
	}
}

func TestReachabilityGraph(t *testing.T) {
	testCases := []struct {
		name                    string
		toCheck                 *pb.NamespaceDefinition
		otherNamespaces         []*pb.NamespaceDefinition
		relationName            string
		expectedEntrypointPaths []string
	}{
		{
			"simple pathing",
			ns.Namespace(
				"document",
				ns.Relation("editor", nil, ns.RelationReference("user", "...")),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
				), ns.RelationReference("user", "...")),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace("user"),
			},
			"editor",
			[]string{"document#editor"},
		},
		{
			"simple computed pathing",
			ns.Namespace(
				"document",
				ns.Relation("editor", nil, ns.RelationReference("user", "...")),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
				), ns.RelationReference("user", "...")),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace("user"),
			},
			"viewer",
			[]string{
				"_this -> union (document#viewer::2) -> document#viewer",
				"document#editor",
			},
		},
		{
			"full pathing with union",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.RelationReference("user", "...")),
				ns.Relation("editor",
					ns.Union(
						ns.This(),
						ns.ComputedUserset("owner"),
					),
					ns.RelationReference("user", "..."),
				),
				ns.Relation("parent", nil, ns.RelationReference("folder", "...")),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "admin"),
				), ns.RelationReference("user", "...")),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"folder",
					ns.Relation("admin", nil, ns.RelationReference("user", "...")),
				),
			},
			"viewer",
			[]string{
				"folder#admin",
				"_this -> union (document#viewer::2) -> document#viewer",
				"_this -> union (document#editor::2) -> document#editor",
				"document#owner",
			},
		},
		{
			"full pathing with intersection",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.RelationReference("user", "...")),
				ns.Relation("editor",
					ns.Union(
						ns.This(),
						ns.ComputedUserset("owner"),
					),
					ns.RelationReference("user", "..."),
				),
				ns.Relation("parent", nil, ns.RelationReference("folder", "...")),
				ns.Relation("viewer", ns.Intersection(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "admin"),
				), ns.RelationReference("user", "...")),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"folder",
					ns.Relation("admin", nil, ns.RelationReference("user", "...")),
				),
			},
			"viewer",
			[]string{
				"_this -> intersection (document#viewer::2) -> document#viewer",
				"_this -> union (document#editor::2) -> document#editor",
				"document#owner",
				"folder#admin",
			},
		},
		{
			"nested groups example",
			ns.Namespace(
				"usergroup",
				ns.Relation("manager",
					nil,
					ns.RelationReference("user", "..."),
					ns.RelationReference("usergroup", "member"),
				),
				ns.Relation("member",
					ns.Union(
						ns.This(),
						ns.ComputedUserset("manager"),
					),
					ns.RelationReference("user", "..."),
					ns.RelationReference("usergroup", "member"),
				),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace("user"),
			},
			"member",
			[]string{
				"_this -> union (usergroup#member::2) -> usergroup#member",
				"usergroup#manager",
			},
		},
		{
			"nested groups for resource",
			ns.Namespace(
				"resource",
				ns.Relation("manager",
					nil,
					ns.RelationReference("user", "..."),
					ns.RelationReference("usergroup", "member"),
					ns.RelationReference("usergroup", "manager"),
				),
				ns.Relation("viewer",
					ns.Union(
						ns.ComputedUserset("manager"),
						ns.This(),
					),
					ns.RelationReference("user", "..."),
					ns.RelationReference("usergroup", "member"),
					ns.RelationReference("usergroup", "manager"),
				),
			),
			[]*pb.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"usergroup",
					ns.Relation("manager",
						nil,
						ns.RelationReference("user", "..."),
						ns.RelationReference("usergroup", "member"),
					),
					ns.Relation("member",
						ns.Union(
							ns.This(),
							ns.ComputedUserset("manager"),
						),
						ns.RelationReference("user", "..."),
						ns.RelationReference("usergroup", "member"),
					),
				),
			},
			"viewer",
			[]string{
				"resource#manager",
				"_this -> union (resource#viewer::2) -> resource#viewer",
				"_this -> union (usergroup#member::2) -> usergroup#member",
				"usergroup#manager",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)

			nsm, err := NewCachingNamespaceManager(ds, 0*time.Second, nil)
			require.NoError(err)

			for _, otherNS := range tc.otherNamespaces {
				_, err := ds.WriteNamespace(context.Background(), otherNS)
				require.NoError(err)
			}

			ts, err := BuildNamespaceTypeSystem(tc.toCheck, nsm)
			require.NoError(err)

			terr := ts.Validate(context.Background())
			require.NoError(terr, "Error in validating namespace")

			graph, err := ts.RelationReachability(context.Background(), tc.relationName)
			require.NoError(err, "Error in building reachability graph")

			foundPaths := []string{}
			for _, entrypoint := range graph.Entrypoints("user", "...") {
				foundPaths = append(foundPaths, entrypoint.DescribePath())
			}
			sort.Strings(foundPaths)

			expectedPaths := tc.expectedEntrypointPaths
			sort.Strings(expectedPaths)

			require.Equal(expectedPaths, foundPaths)
		})
	}
}
