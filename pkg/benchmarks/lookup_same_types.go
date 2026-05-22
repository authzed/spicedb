package benchmarks

import (
	"context"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// LookupSameTypes benchmarks permission checking on resources where multiple
// relations (viewer, editor, creator, owner) all contribute to the same
// permission, and usergroups have overlapping membership paths
// (direct_member, contributor, manager).
//
// Schema: user + resource (viewer, editor, creator, owner;
//
//	view = viewer + editor + creator + owner) +
//	usergroup (direct_member, contributor, manager;
//	  member = direct_member + contributor + manager)
//
// Structure:
//   - Nested usergroups: productname -> applications -> engineering, plus csuite
//   - resource:promserver has creator@user:an_engineer and viewer@engineering#member
//   - resource:jira has viewer@engineering#member
//   - An external user and an unrelated "other" group
func init() {
	registerBenchmark(Benchmark{
		Name:  "LookupSameTypes",
		Tags:  []Tag{Recursion},
		Setup: setupLookupSameTypes,
	})
}

func lookupSameTypesRelationships() []tuple.Relationship {
	return []tuple.Relationship{
		// Usergroup managers
		tuple.MustParse("usergroup:productname#manager@user:an_eng_manager"),
		tuple.MustParse("usergroup:applications#manager@user:an_eng_director"),
		tuple.MustParse("usergroup:engineering#manager@user:cto"),
		tuple.MustParse("usergroup:csuite#manager@user:ceo"),

		// Usergroup direct members
		tuple.MustParse("usergroup:productname#direct_member@user:an_engineer"),
		tuple.MustParse("usergroup:csuite#direct_member@user:cto"),
		tuple.MustParse("usergroup:other#direct_member@user:denied"),

		// Nested group membership
		tuple.MustParse("usergroup:engineering#direct_member@usergroup:applications#member"),
		tuple.MustParse("usergroup:applications#direct_member@usergroup:productname#member"),
		tuple.MustParse("usergroup:engineering#direct_member@usergroup:csuite#member"),

		// Resource permissions
		tuple.MustParse("resource:promserver#creator@user:an_engineer"),
		tuple.MustParse("resource:promserver#viewer@usergroup:engineering#member"),
		tuple.MustParse("resource:jira#viewer@usergroup:engineering#member"),
		tuple.MustParse("resource:promserver#viewer@user:an_external_user"),
	}
}

func setupLookupSameTypes(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	schemaText := `
		definition user {}

		definition resource {
			relation viewer: user | usergroup#member | usergroup#manager
			relation editor: user | usergroup#member | usergroup#manager
			relation creator: user | usergroup#member | usergroup#manager
			relation owner: user | usergroup#member | usergroup#manager

			permission view = viewer + editor + creator + owner
		}

		definition usergroup {
			relation direct_member: user | usergroup#member | usergroup#manager | usergroup#contributor
			relation contributor: user | usergroup#member | usergroup#contributor | usergroup#manager
			relation manager: user | usergroup#member | usergroup#manager
			permission member = direct_member + contributor + manager
		}
	`

	_, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText)
	if err != nil {
		return nil, err
	}

	_, err = writeRelationships(ctx, ds, lookupSameTypesRelationships())
	if err != nil {
		return nil, err
	}

	return &QuerySets{
		Checks: []CheckQuery{
			{
				ResourceType:    "resource",
				ResourceID:      "promserver",
				Permission:      "view",
				SubjectType:     "user",
				SubjectID:       "an_engineer",
				SubjectRelation: tuple.Ellipsis,
			},
		},
		IterResources: []IterResourcesQuery{
			{
				SubjectType:         "user",
				SubjectID:           "an_engineer",
				SubjectRelation:     tuple.Ellipsis,
				Permission:          "view",
				FilterResourceType:  "resource",
				ExpectedResourceIDs: []string{"promserver", "jira"},
			},
		},
		IterSubjects: []IterSubjectsQuery{
			{
				ResourceType:       "resource",
				ResourceID:         "promserver",
				Permission:         "view",
				FilterSubjectType:  "user",
				ExpectedSubjectIDs: []string{"an_engineer", "an_eng_manager", "an_eng_director", "cto", "ceo", "an_external_user"},
			},
		},
	}, nil
}
