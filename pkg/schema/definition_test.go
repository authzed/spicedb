package schema

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestDefinition(t *testing.T) {
	emptyEnv := caveats.NewEnvironment()

	testCases := []struct {
		name            string
		toCheck         *core.NamespaceDefinition
		otherNamespaces []*core.NamespaceDefinition
		caveats         []*core.CaveatDefinition
		expectedError   string
	}{
		{
			"invalid relation in computed_userset",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("parent", nil),
				ns.MustRelation("lock", nil),
				ns.MustRelation("viewer", ns.Union(
					ns.ComputedUserset("editors"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			nil,
			"relation/permission `editors` not found under definition `document`",
		},
		{
			"invalid relation in tuple_to_userset",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("parent", nil),
				ns.MustRelation("lock", nil),
				ns.MustRelation("viewer", ns.Union(
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parents", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			nil,
			"relation/permission `parents` not found under definition `document`",
		},
		{
			"use of permission in tuple_to_userset",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("parent", nil),
				ns.MustRelation("lock", nil),
				ns.MustRelation("viewer", ns.Union(
					ns.TupleToUserset("editor", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			nil,
			"under permission `viewer` under definition `document`: permissions cannot be used on the left hand side of an arrow (found `editor`)",
		},
		{
			"rewrite without this and types",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("document", "owner")),
			),
			[]*core.NamespaceDefinition{},
			nil,
			"direct relations are not allowed under relation `editor`",
		},
		{
			"relation in relation types has invalid namespace",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil, ns.AllowedRelation("someinvalidns", "...")),
				ns.MustRelation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("parent", nil),
				ns.MustRelation("lock", nil),
				ns.MustRelation("viewer", ns.Union(
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			nil,
			"could not lookup definition `someinvalidns` for relation `owner`: object definition `someinvalidns` not found",
		},
		{
			"relation in relation types has invalid relation",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil, ns.AllowedRelation("anotherns", "foobar")),
				ns.MustRelation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("parent", nil),
				ns.MustRelation("lock", nil),
				ns.MustRelation("viewer", ns.Union(
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"anotherns",
				),
			},
			nil,
			"relation/permission `foobar` not found under definition `anotherns`",
		},
		{
			"full type check",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil, ns.AllowedRelation("user", "...")),
				ns.MustRelation("can_comment",
					nil,
					ns.AllowedRelation("user", "..."),
					ns.AllowedRelation("folder", "can_comment"),
				),
				ns.MustRelation("editor",
					ns.Union(
						ns.ComputedUserset("owner"),
					),
				),
				ns.MustRelation("parent", nil, ns.AllowedRelation("folder", "...")),
				ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedPublicNamespace("user")),
				ns.MustRelation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "view"),
				)),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"folder",
					ns.MustRelation("can_comment", nil, ns.AllowedRelation("user", "...")),
					ns.MustRelation("parent", nil, ns.AllowedRelation("folder", "...")),
				),
			},
			nil,
			"",
		},
		{
			"transitive wildcard type check",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelation("group", "member")),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"group",
					ns.MustRelation("member", nil, ns.AllowedRelation("user", "..."), ns.AllowedPublicNamespace("user")),
				),
			},
			nil,
			"for relation `viewer`: relation/permission `group#member` includes wildcard type `user` via relation `group#member`: wildcard relations cannot be transitively included",
		},
		{
			"ttu wildcard type check",
			ns.Namespace(
				"folder",
				ns.MustRelation("parent", nil, ns.AllowedRelation("folder", "..."), ns.AllowedPublicNamespace("folder")),
				ns.MustRelation("viewer", ns.Union(
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			nil,
			"for arrow under permission `viewer`: relation `folder#parent` includes wildcard type `folder` via relation `folder#parent`: wildcard relations cannot be used on the left side of arrows",
		},
		{
			"recursive transitive wildcard type check",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelation("group", "member")),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"group",
					ns.MustRelation("member", nil, ns.AllowedRelation("group", "manager"), ns.AllowedRelation("user", "...")),
					ns.MustRelation("manager", nil, ns.AllowedRelation("group", "member"), ns.AllowedPublicNamespace("user")),
				),
			},
			nil,
			"for relation `viewer`: relation/permission `group#member` includes wildcard type `user` via relation `group#manager`: wildcard relations cannot be transitively included",
		},
		{
			"redefinition of allowed relation",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelation("user", "...")),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			nil,
			"found duplicate allowed subject type `user` on relation `viewer` under definition `document`",
		},
		{
			"redefinition of allowed public relation",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedPublicNamespace("user"), ns.AllowedPublicNamespace("user")),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			nil,
			"found duplicate allowed subject type `user:*` on relation `viewer` under definition `document`",
		},
		{
			"no redefinition of allowed relation",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedPublicNamespace("user"), ns.AllowedRelation("user", "..."), ns.AllowedRelation("user", "viewer")),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user", ns.MustRelation("viewer", nil)),
			},
			nil,
			"",
		},
		{
			"unknown caveat",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("unknown"))),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			nil,
			"could not lookup caveat `unknown` for relation `viewer`: caveat with name `unknown` not found",
		},
		{
			"valid caveat",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat"))),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"",
		},
		{
			"valid optional caveat",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat"))),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"",
		},
		{
			"duplicate caveat",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")), ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat"))),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"found duplicate allowed subject type `user with definedcaveat` on relation `viewer` under definition `document`",
		},
		{
			"valid wildcard caveat",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedPublicNamespaceWithCaveat("user", ns.AllowedCaveat("definedcaveat"))),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"",
		},
		{
			"valid all the caveats",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelation("user", "..."),
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
					ns.AllowedPublicNamespaceWithCaveat("user", ns.AllowedCaveat("definedcaveat")),
					ns.AllowedRelationWithCaveat("team", "member", ns.AllowedCaveat("definedcaveat")),
				),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace("team",
					ns.MustRelation("member", nil),
				),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"",
		},
		{
			"valid expiration",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelationWithExpiration("user", "..."),
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
				),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace("team",
					ns.MustRelation("member", nil),
				),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"",
		},
		{
			"duplicate expiration",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelationWithExpiration("user", "..."),
					ns.AllowedRelationWithExpiration("user", "..."),
				),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace("team",
					ns.MustRelation("member", nil),
				),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"found duplicate allowed subject type `user with expiration` on relation `viewer` under definition `document`",
		},
		{
			"non-duplicate expiration",
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelationWithExpiration("user", "..."),
					ns.AllowedRelationWithExpiration("team", "..."),
				),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace("team",
					ns.MustRelation("member", nil),
				),
			},
			[]*core.CaveatDefinition{
				ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
			},
			"",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := context.Background()

			lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				err := rwt.WriteNamespaces(ctx, tc.toCheck)
				if err != nil {
					return err
				}
				for _, otherNS := range tc.otherNamespaces {
					if err := rwt.WriteNamespaces(ctx, otherNS); err != nil {
						return err
					}
				}
				cw := rwt.(datastore.CaveatStorer)
				return cw.WriteCaveats(ctx, tc.caveats)
			})
			require.NoError(err)
			resolver := ResolverForDatastoreReader(ds.SnapshotReader(lastRevision))

			ts := NewTypeSystem(resolver)

			def, gerr := ts.GetDefinition(ctx, tc.toCheck.GetName())
			require.NoError(gerr)

			_, verr := def.Validate(ctx)
			if tc.expectedError == "" {
				require.NoError(verr)
			} else {
				require.Error(verr)
				require.Equal(tc.expectedError, verr.Error())
			}
		})
	}
}

type tsTester func(t *testing.T, ts *ValidatedDefinition)

func noError[T any](result T, err error) T {
	if err != nil {
		panic(err)
	}

	return result
}

func requireSameAllowedRelations(t *testing.T, found []*core.AllowedRelation, expected ...*core.AllowedRelation) {
	foundSet := mapz.NewSet[string]()
	for _, f := range found {
		foundSet.Add(SourceForAllowedRelation(f))
	}

	expectSet := mapz.NewSet[string]()
	for _, e := range expected {
		expectSet.Add(SourceForAllowedRelation(e))
	}

	foundSlice := foundSet.AsSlice()
	expectedSlice := expectSet.AsSlice()

	sort.Strings(foundSlice)
	sort.Strings(expectedSlice)

	require.Equal(t, expectedSlice, foundSlice)
}

func requireSameSubjectRelations(t *testing.T, found []*core.RelationReference, expected ...*core.RelationReference) {
	foundSet := mapz.NewSet[string]()
	for _, f := range found {
		foundSet.Add(tuple.StringCoreRR(f))
	}

	expectSet := mapz.NewSet[string]()
	for _, e := range expected {
		expectSet.Add(tuple.StringCoreRR(e))
	}

	foundSlice := foundSet.AsSlice()
	expectedSlice := expectSet.AsSlice()

	sort.Strings(foundSlice)
	sort.Strings(expectedSlice)

	require.Equal(t, expectedSlice, foundSlice)
}

func TestTypeSystemAccessors(t *testing.T) {
	tcs := []struct {
		name       string
		schema     string
		namespaces map[string]tsTester
	}{
		{
			"basic schema",
			`definition user {}

			definition resource {
				relation editor: user
				relation viewer: user

				permission edit = editor
				permission view = viewer + edit
			}`,
			map[string]tsTester{
				"user": func(t *testing.T, vts *ValidatedDefinition) {
					require.False(t, vts.IsPermission("somenonpermission"))
				},
				"resource": func(t *testing.T, vts *ValidatedDefinition) {
					t.Run("IsPermission", func(t *testing.T) {
						require.False(t, vts.IsPermission("somenonpermission"))

						require.False(t, vts.IsPermission("viewer"))
						require.False(t, vts.IsPermission("editor"))

						require.True(t, vts.IsPermission("view"))
						require.True(t, vts.IsPermission("edit"))
					})

					t.Run("RelationDoesNotAllowCaveatsOrTraitsForSubject", func(t *testing.T) {
						ok, err := vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("viewer", "user")
						require.NoError(t, err)
						require.True(t, ok)

						ok, err = vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("editor", "user")
						require.NoError(t, err)
						require.True(t, ok)
					})

					t.Run("IsAllowedPublicNamespace", func(t *testing.T) {
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("editor", "user")))
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("viewer", "user")))

						_, err := vts.IsAllowedPublicNamespace("unknown", "user")
						require.Error(t, err)
					})

					t.Run("IsAllowedDirectNamespace", func(t *testing.T) {
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("editor", "user")))
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("viewer", "user")))

						_, err := vts.IsAllowedPublicNamespace("unknown", "user")
						require.Error(t, err)
					})

					t.Run("GetAllowedDirectNamespaceSubjectRelations", func(t *testing.T) {
						require.Equal(t, []string{"..."}, noError(vts.GetAllowedDirectNamespaceSubjectRelations("editor", "user")).AsSlice())
						_, err := vts.GetAllowedDirectNamespaceSubjectRelations("unknown", "user")
						require.Error(t, err)
					})

					t.Run("IsAllowedDirectRelation", func(t *testing.T) {
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("editor", "user", "...")))
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("viewer", "user", "...")))

						require.Equal(t, DirectRelationNotValid, noError(vts.IsAllowedDirectRelation("editor", "user", "other")))
						require.Equal(t, DirectRelationNotValid, noError(vts.IsAllowedDirectRelation("viewer", "user", "other")))

						_, err := vts.IsAllowedDirectRelation("unknown", "user", "...")
						require.Error(t, err)
					})

					t.Run("HasAllowedRelation", func(t *testing.T) {
						userDirect := ns.AllowedRelation("user", "...")
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("editor", userDirect)))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", userDirect)))

						userWithCaveat := ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("editor", userWithCaveat)))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("viewer", userWithCaveat)))

						_, err := vts.HasAllowedRelation("unknown", userDirect)
						require.Error(t, err)
					})

					t.Run("AllowedDirectRelationsAndWildcards", func(t *testing.T) {
						userDirect := ns.AllowedRelation("user", "...")
						allowed := noError(vts.AllowedDirectRelationsAndWildcards("editor"))
						requireSameAllowedRelations(t, allowed, userDirect)

						_, err := vts.AllowedDirectRelationsAndWildcards("unknown")
						require.Error(t, err)
					})

					t.Run("AllowedSubjectRelations", func(t *testing.T) {
						userDirect := ns.RelationReference("user", "...")
						allowed := noError(vts.AllowedSubjectRelations("editor"))
						requireSameSubjectRelations(t, allowed, userDirect)

						_, err := vts.AllowedSubjectRelations("unknown")
						require.Error(t, err)
					})
				},
			},
		},
		{
			"schema with wildcards",
			`definition user {}

			definition resource {
				relation editor: user
				relation viewer: user | user:*
				permission view = viewer + editor
			}`,
			map[string]tsTester{
				"resource": func(t *testing.T, vts *ValidatedDefinition) {
					t.Run("IsPermission", func(t *testing.T) {
						require.False(t, vts.IsPermission("viewer"))
						require.True(t, vts.IsPermission("view"))
					})

					t.Run("RelationDoesNotAllowCaveatsOrTraitsForSubject", func(t *testing.T) {
						ok, err := vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("viewer", "user")
						require.NoError(t, err)
						require.True(t, ok)

						ok, err = vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("editor", "user")
						require.NoError(t, err)
						require.True(t, ok)
					})

					t.Run("IsAllowedPublicNamespace", func(t *testing.T) {
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("editor", "user")))
						require.Equal(t, PublicSubjectAllowed, noError(vts.IsAllowedPublicNamespace("viewer", "user")))
					})

					t.Run("IsAllowedDirectNamespace", func(t *testing.T) {
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("editor", "user")))
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("viewer", "user")))
					})

					t.Run("GetAllowedDirectNamespaceSubjectRelations", func(t *testing.T) {
						require.Equal(t, []string{"..."}, noError(vts.GetAllowedDirectNamespaceSubjectRelations("viewer", "user")).AsSlice())
						_, err := vts.GetAllowedDirectNamespaceSubjectRelations("unknown", "user")
						require.Error(t, err)
					})

					t.Run("IsAllowedDirectRelation", func(t *testing.T) {
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("editor", "user", "...")))
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("viewer", "user", "...")))
					})

					t.Run("HasAllowedRelation", func(t *testing.T) {
						userDirect := ns.AllowedRelation("user", "...")
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("editor", userDirect)))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", userDirect)))

						userWildcard := ns.AllowedPublicNamespace("user")
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("editor", userWildcard)))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", userWildcard)))
					})

					t.Run("AllowedDirectRelationsAndWildcards", func(t *testing.T) {
						userDirect := ns.AllowedRelation("user", "...")
						userWildcard := ns.AllowedPublicNamespace("user")

						allowed := noError(vts.AllowedDirectRelationsAndWildcards("editor"))
						requireSameAllowedRelations(t, allowed, userDirect)

						allowed = noError(vts.AllowedDirectRelationsAndWildcards("viewer"))
						requireSameAllowedRelations(t, allowed, userDirect, userWildcard)
					})

					t.Run("AllowedSubjectRelations", func(t *testing.T) {
						userDirect := ns.RelationReference("user", "...")
						allowed := noError(vts.AllowedSubjectRelations("viewer"))
						requireSameSubjectRelations(t, allowed, userDirect)
					})
				},
			},
		},
		{
			"schema with subject relations",
			`definition user {}

			definition thirdtype {}

			definition group {
				relation member: user | group#member
				relation other: user
				relation three: user | group#member | group#other
			}`,
			map[string]tsTester{
				"group": func(t *testing.T, vts *ValidatedDefinition) {
					t.Run("IsPermission", func(t *testing.T) {
						require.False(t, vts.IsPermission("member"))
					})

					t.Run("RelationDoesNotAllowCaveatsOrTraitsForSubject", func(t *testing.T) {
						ok, err := vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("member", "user")
						require.NoError(t, err)
						require.True(t, ok)

						ok, err = vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("member", "group")
						require.NoError(t, err)
						require.True(t, ok)
					})

					t.Run("IsAllowedPublicNamespace", func(t *testing.T) {
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("member", "user")))
					})

					t.Run("IsAllowedDirectNamespace", func(t *testing.T) {
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("member", "user")))
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("member", "group")))
						require.Equal(t, AllowedDefinitionNotValid, noError(vts.IsAllowedDirectNamespace("member", "thirdtype")))
					})

					t.Run("GetAllowedDirectNamespaceSubjectRelations", func(t *testing.T) {
						require.Equal(t, []string{"..."}, noError(vts.GetAllowedDirectNamespaceSubjectRelations("member", "user")).AsSlice())
						require.Equal(t, []string{"member"}, noError(vts.GetAllowedDirectNamespaceSubjectRelations("member", "group")).AsSlice())
						require.ElementsMatch(t, []string{"member", "other"}, noError(vts.GetAllowedDirectNamespaceSubjectRelations("three", "group")).AsSlice())
						_, err := vts.GetAllowedDirectNamespaceSubjectRelations("unknown", "user")
						require.Error(t, err)
					})

					t.Run("IsAllowedDirectRelation", func(t *testing.T) {
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("member", "user", "...")))
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("member", "group", "member")))
						require.Equal(t, DirectRelationNotValid, noError(vts.IsAllowedDirectRelation("member", "group", "...")))
					})

					t.Run("HasAllowedRelation", func(t *testing.T) {
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("member", ns.AllowedRelation("user", "..."))))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("member", ns.AllowedRelation("group", "member"))))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("member", ns.AllowedRelation("group", "..."))))
					})

					t.Run("AllowedDirectRelationsAndWildcards", func(t *testing.T) {
						userDirect := ns.AllowedRelation("user", "...")
						groupMember := ns.AllowedRelation("group", "member")

						allowed := noError(vts.AllowedDirectRelationsAndWildcards("member"))
						requireSameAllowedRelations(t, allowed, userDirect, groupMember)
					})

					t.Run("AllowedSubjectRelations", func(t *testing.T) {
						userDirect := ns.RelationReference("user", "...")
						groupMember := ns.RelationReference("group", "member")

						allowed := noError(vts.AllowedSubjectRelations("member"))
						requireSameSubjectRelations(t, allowed, userDirect, groupMember)
					})
				},
			},
		},
		{
			"schema with caveats",
			`definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition resource {
				relation editor: user
				relation viewer: user | user with somecaveat
				relation onlycaveated: user with somecaveat
			}`,
			map[string]tsTester{
				"resource": func(t *testing.T, vts *ValidatedDefinition) {
					t.Run("IsPermission", func(t *testing.T) {
						require.False(t, vts.IsPermission("editor"))
						require.False(t, vts.IsPermission("viewer"))
						require.False(t, vts.IsPermission("onlycaveated"))
					})

					t.Run("RelationDoesNotAllowCaveatsOrTraitsForSubject", func(t *testing.T) {
						ok, err := vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("viewer", "user")
						require.NoError(t, err)
						require.False(t, ok)

						ok, err = vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("editor", "user")
						require.NoError(t, err)
						require.True(t, ok)

						ok, err = vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("onlycaveated", "user")
						require.NoError(t, err)
						require.False(t, ok)
					})

					t.Run("IsAllowedPublicNamespace", func(t *testing.T) {
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("editor", "user")))
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("viewer", "user")))
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("onlycaveated", "user")))
					})

					t.Run("IsAllowedDirectNamespace", func(t *testing.T) {
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("editor", "user")))
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("viewer", "user")))
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("onlycaveated", "user")))
					})

					t.Run("IsAllowedDirectRelation", func(t *testing.T) {
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("editor", "user", "...")))
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("viewer", "user", "...")))
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("onlycaveated", "user", "...")))
					})

					t.Run("HasAllowedRelation", func(t *testing.T) {
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("editor", ns.AllowedRelation("user", "..."))))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", ns.AllowedRelation("user", "..."))))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("onlycaveated", ns.AllowedRelation("user", "..."))))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("viewer", ns.AllowedRelationWithExpiration("user", "..."))))

						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")))))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("onlycaveated", ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")))))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("onlycaveated", ns.AllowedRelationWithCaveatAndExpiration("user", "...", ns.AllowedCaveat("somecaveat")))))
					})

					t.Run("AllowedDirectRelationsAndWildcards", func(t *testing.T) {
						userDirect := ns.AllowedRelation("user", "...")
						caveatedUser := ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))

						allowed := noError(vts.AllowedDirectRelationsAndWildcards("editor"))
						requireSameAllowedRelations(t, allowed, userDirect)

						allowed = noError(vts.AllowedDirectRelationsAndWildcards("viewer"))
						requireSameAllowedRelations(t, allowed, userDirect, caveatedUser)

						allowed = noError(vts.AllowedDirectRelationsAndWildcards("onlycaveated"))
						requireSameAllowedRelations(t, allowed, caveatedUser)
					})

					t.Run("AllowedSubjectRelations", func(t *testing.T) {
						userDirect := ns.RelationReference("user", "...")

						allowed := noError(vts.AllowedSubjectRelations("editor"))
						requireSameSubjectRelations(t, allowed, userDirect)

						allowed = noError(vts.AllowedSubjectRelations("viewer"))
						requireSameSubjectRelations(t, allowed, userDirect)

						allowed = noError(vts.AllowedSubjectRelations("onlycaveated"))
						requireSameSubjectRelations(t, allowed, userDirect)
					})
				},
			},
		},
		{
			"schema with expiration",
			`use expiration

			definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition resource {
				relation editor: user with expiration
				relation viewer: user | user with somecaveat | user with expiration | user with somecaveat and expiration
			}`,
			map[string]tsTester{
				"resource": func(t *testing.T, vts *ValidatedDefinition) {
					t.Run("IsPermission", func(t *testing.T) {
						require.False(t, vts.IsPermission("editor"))
						require.False(t, vts.IsPermission("viewer"))
					})

					t.Run("RelationDoesNotAllowCaveatsOrTraitsForSubject", func(t *testing.T) {
						ok, err := vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("editor", "user")
						require.NoError(t, err)
						require.False(t, ok)

						ok, err = vts.RelationDoesNotAllowCaveatsOrTraitsForSubject("viewer", "user")
						require.NoError(t, err)
						require.False(t, ok)
					})

					t.Run("IsAllowedPublicNamespace", func(t *testing.T) {
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("editor", "user")))
						require.Equal(t, PublicSubjectNotAllowed, noError(vts.IsAllowedPublicNamespace("viewer", "user")))
					})

					t.Run("IsAllowedDirectNamespace", func(t *testing.T) {
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("editor", "user")))
						require.Equal(t, AllowedDefinitionValid, noError(vts.IsAllowedDirectNamespace("viewer", "user")))
					})

					t.Run("IsAllowedDirectRelation", func(t *testing.T) {
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("editor", "user", "...")))
						require.Equal(t, DirectRelationValid, noError(vts.IsAllowedDirectRelation("viewer", "user", "...")))
					})

					t.Run("HasAllowedRelation", func(t *testing.T) {
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("editor", ns.AllowedRelationWithExpiration("user", "..."))))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("editor", ns.AllowedRelation("user", "..."))))
						require.Equal(t, AllowedRelationNotValid, noError(vts.HasAllowedRelation("editor", ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")))))

						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", ns.AllowedRelation("user", "..."))))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")))))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", ns.AllowedRelationWithExpiration("user", "..."))))
						require.Equal(t, AllowedRelationValid, noError(vts.HasAllowedRelation("viewer", ns.AllowedRelationWithCaveatAndExpiration("user", "...", ns.AllowedCaveat("somecaveat")))))
					})

					t.Run("AllowedDirectRelationsAndWildcards", func(t *testing.T) {
						userDirect := ns.AllowedRelation("user", "...")
						caveatedUser := ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))
						expiringUser := ns.AllowedRelationWithExpiration("user", "...")
						expiringCaveatedUser := ns.AllowedRelationWithCaveatAndExpiration("user", "...", ns.AllowedCaveat("somecaveat"))

						allowed := noError(vts.AllowedDirectRelationsAndWildcards("editor"))
						requireSameAllowedRelations(t, allowed, expiringUser)

						allowed = noError(vts.AllowedDirectRelationsAndWildcards("viewer"))
						requireSameAllowedRelations(t, allowed, userDirect, caveatedUser, expiringUser, expiringCaveatedUser)
					})

					t.Run("AllowedSubjectRelations", func(t *testing.T) {
						userDirect := ns.RelationReference("user", "...")

						allowed := noError(vts.AllowedSubjectRelations("editor"))
						requireSameSubjectRelations(t, allowed, userDirect)

						allowed = noError(vts.AllowedSubjectRelations("viewer"))
						requireSameSubjectRelations(t, allowed, userDirect)
					})
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := datastoremw.ContextWithDatastore(context.Background(), ds)

			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.schema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			lastRevision, err := ds.HeadRevision(context.Background())
			require.NoError(err)

			reader := ds.SnapshotReader(lastRevision)
			resolver := ResolverForDatastoreReader(reader).WithPredefinedElements(PredefinedElements{
				Definitions: compiled.ObjectDefinitions,
				Caveats:     compiled.CaveatDefinitions,
			})
			ts := NewTypeSystem(resolver)
			for _, nsDef := range compiled.ObjectDefinitions {
				vts, err := ts.GetValidatedDefinition(ctx, nsDef.GetName())
				require.NoError(err)

				require.Equal(vts.Namespace(), nsDef)

				tester, ok := tc.namespaces[nsDef.Name]
				if ok {
					tester := tester
					vts := vts
					t.Run(nsDef.Name, func(t *testing.T) {
						for _, relation := range nsDef.Relation {
							require.True(vts.IsPermission(relation.Name) || vts.HasTypeInformation(relation.Name))
						}

						tester(t, vts)
					})
				}
			}
		})
	}
}
