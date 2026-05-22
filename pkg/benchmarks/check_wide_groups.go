package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// CheckWideGroups benchmarks checking a permission through nested group
// hierarchies with recursive membership.
//
// Schema: user + group (member: user | group#member) +
//
//	resource (viewer: user | group#member, view = viewer)
//
// Structure:
//   - resource:someresource has viewer = group:eng#member
//   - group:eng has ~80 sub-groups (eng-11 through eng-99, skipping tens digits)
//   - Each sub-group eng-X has one child eng-X-1
//   - user:tom is a member of eng-99-1 (at the deepest leaf)
//
// This tests recursive group membership resolution through a wide + deep
// hierarchy.
func init() {
	registerBenchmark(Benchmark{
		Name:  "CheckWideGroups",
		Tags:  []Tag{Recursion},
		Setup: setupCheckWideGroups,
	})
}

func setupCheckWideGroups(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	schemaText := `
		definition user {}

		definition group {
			relation member: user | group#member
		}

		definition resource {
			relation viewer: user | group#member
			permission view = viewer
		}
	`

	_, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText)
	if err != nil {
		return nil, err
	}

	var relationships []tuple.Relationship

	// resource:someresource#viewer@group:eng#member
	relationships = append(relationships, tuple.MustParse("resource:someresource#viewer@group:eng#member"))

	// group:eng has sub-groups eng-XY where X is 1-9 and Y is 1-9
	// (i.e. eng-11 through eng-99, skipping multiples of 10)
	for x := 1; x <= 9; x++ {
		for y := 1; y <= 9; y++ {
			subgroup := fmt.Sprintf("eng-%d%d", x, y)
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("group:eng#member@group:%s#member", subgroup)))
		}
	}

	// Each eng-N has one child eng-N-1
	for i := 1; i <= 99; i++ {
		relationships = append(relationships, tuple.MustParse(
			fmt.Sprintf("group:eng-%d#member@group:eng-%d-1#member", i, i)))
	}

	// user:tom is at the deepest leaf
	relationships = append(relationships, tuple.MustParse("group:eng-99-1#member@user:tom"))

	_, err = writeRelationships(ctx, ds, relationships)
	if err != nil {
		return nil, err
	}

	return &QuerySets{
		Checks: []CheckQuery{
			{
				ResourceType:    "resource",
				ResourceID:      "someresource",
				Permission:      "view",
				SubjectType:     "user",
				SubjectID:       "tom",
				SubjectRelation: tuple.Ellipsis,
			},
		},
		IterSubjects: []IterSubjectsQuery{
			{
				ResourceType:       "resource",
				ResourceID:         "someresource",
				Permission:         "view",
				FilterSubjectType:  "user",
				ExpectedSubjectIDs: []string{"tom"},
			},
		},
	}, nil
}
