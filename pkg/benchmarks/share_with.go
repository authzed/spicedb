package benchmarks

import (
	"context"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ShareWith benchmarks a realistic organizational model with nested user groups,
// resources, and an organization definition that aggregates them.
//
// Schema: user + resource (manager, viewer, view = viewer + manager) +
//
//	usergroup (manager, direct_member, member = direct_member + manager) +
//	organization (group, resource, admin, member = admin + group->member,
//	  user = admin + member + resource->view)
//
// Structure:
//   - Nested usergroups: productname -> applications -> engineering, plus csuite
//   - Organization:someorg aggregates all groups and resources
//   - Resources: promserver (managed by productname, viewed by engineering) and
//     jira (viewed + managed by engineering)
//   - External user and unrelated villain group for negative cases
func init() {
	registerBenchmark(Benchmark{
		Name:  "ShareWith",
		Tags:  []Tag{Arrows, Recursion},
		Setup: setupShareWith,
	})
}

func shareWithRelationships() []tuple.Relationship {
	return []tuple.Relationship{
		// Usergroup managers
		tuple.MustParse("usergroup:productname#manager@user:an_eng_manager"),
		tuple.MustParse("usergroup:applications#manager@user:an_eng_director"),
		tuple.MustParse("usergroup:engineering#manager@user:cto"),
		tuple.MustParse("usergroup:csuite#manager@user:ceo"),

		// Usergroup direct members
		tuple.MustParse("usergroup:productname#direct_member@user:an_engineer"),
		tuple.MustParse("usergroup:csuite#direct_member@user:cto"),

		// Nested group membership
		tuple.MustParse("usergroup:engineering#direct_member@usergroup:applications#member"),
		tuple.MustParse("usergroup:applications#direct_member@usergroup:productname#member"),
		tuple.MustParse("usergroup:engineering#direct_member@usergroup:csuite#member"),

		// Organization structure
		tuple.MustParse("organization:someorg#group@usergroup:csuite"),
		tuple.MustParse("organization:someorg#group@usergroup:productname"),
		tuple.MustParse("organization:someorg#group@usergroup:applications"),
		tuple.MustParse("organization:someorg#group@usergroup:engineering"),
		tuple.MustParse("organization:someorg#resource@resource:promserver"),
		tuple.MustParse("organization:someorg#resource@resource:jira"),
		tuple.MustParse("organization:someorg#admin@usergroup:csuite#member"),
		tuple.MustParse("organization:someorg#admin@user:it_admin"),

		// Resource permissions
		tuple.MustParse("resource:promserver#manager@usergroup:productname#member"),
		tuple.MustParse("resource:promserver#viewer@usergroup:engineering#member"),
		tuple.MustParse("resource:jira#viewer@usergroup:engineering#member"),
		tuple.MustParse("resource:jira#manager@usergroup:engineering#manager"),
		tuple.MustParse("resource:promserver#viewer@user:an_external_user"),

		// Unrelated group
		tuple.MustParse("usergroup:blackhats#manager@user:a_villain"),
	}
}

func setupShareWith(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	schemaText := `
		definition user {}

		definition resource {
			relation manager: user | usergroup#member | usergroup#manager
			relation viewer: user | usergroup#member | usergroup#manager
			permission view = viewer + manager
		}

		definition usergroup {
			relation manager: user | usergroup#member | usergroup#manager
			relation direct_member: user | usergroup#member | usergroup#manager
			permission member = direct_member + manager
		}

		definition organization {
			relation group: usergroup
			relation resource: resource
			relation admin: user | usergroup#member | usergroup#manager
			permission member = admin + group->member
			permission user = admin + member + resource->view
		}
	`

	_, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText)
	if err != nil {
		return nil, err
	}

	_, err = writeRelationships(ctx, ds, shareWithRelationships())
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
	}, nil
}
