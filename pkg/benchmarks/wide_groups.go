package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// WideGroups benchmarks recursive lookup through wide group structures.
//
// Schema: user + group (member: user | group#member) +
//
//	resource (viewer: user | group#member, view = viewer)
//
// Structure:
//   - group:first has user:tom as a direct member
//   - group:second#member has group:first#member
//   - group:third#member has group:second#member
//   - resource:someresource#viewer has group:third#member
//   - 100 wide groups at each of 3 levels (widegroup1-N, widegroup2-N, widegroup3-N)
//     each chaining to the previous level's corresponding group
//   - resource:anotherresource#viewer has group:widegroup3-99#member
//   - resource:thirdresource#viewer has user:sarah
//
// This tests recursive group membership resolution with wide fan-out at
// multiple levels.
func init() {
	registerBenchmark(Benchmark{
		Name:  "WideGroups",
		Tags:  []Tag{Recursion},
		Setup: setupWideGroups,
	})
}

func setupWideGroups(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	const numWideGroups = 100

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

	// Core chain: first -> second -> third -> resource
	relationships = append(relationships,
		tuple.MustParse("group:first#member@user:tom"),
		tuple.MustParse("group:second#member@group:first#member"),
		tuple.MustParse("group:third#member@group:second#member"),
		tuple.MustParse("resource:someresource#viewer@group:third#member"),
	)

	// 100 wide groups at level 1, all containing group:first#member
	for i := 0; i < numWideGroups; i++ {
		relationships = append(relationships, tuple.MustParse(
			fmt.Sprintf("group:widegroup1-%d#member@group:first#member", i)))
	}

	// 100 wide groups at level 2, each containing the corresponding level 1 group
	for i := 0; i < numWideGroups; i++ {
		relationships = append(relationships, tuple.MustParse(
			fmt.Sprintf("group:widegroup2-%d#member@group:widegroup1-%d#member", i, i)))
	}

	// 100 wide groups at level 3, each containing the corresponding level 2 group
	for i := 0; i < numWideGroups; i++ {
		relationships = append(relationships, tuple.MustParse(
			fmt.Sprintf("group:widegroup3-%d#member@group:widegroup2-%d#member", i, i)))
	}

	// Additional resources
	relationships = append(relationships,
		tuple.MustParse("resource:anotherresource#viewer@group:widegroup3-99#member"),
		tuple.MustParse("resource:thirdresource#viewer@user:sarah"),
	)

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
		IterResources: []IterResourcesQuery{
			{
				SubjectType:         "user",
				SubjectID:           "tom",
				SubjectRelation:     tuple.Ellipsis,
				Permission:          "view",
				FilterResourceType:  "resource",
				ExpectedResourceIDs: []string{"someresource", "anotherresource"},
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
