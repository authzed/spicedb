package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// LookupIntersection benchmarks lookup operations with intersection logic.
//
// Schema:
//
//	definition organization { relation reader: user; relation banned: user;
//	  permission read = reader - banned }
//	definition resource { relation viewer: user; relation organization: organization;
//	  permission read = organization->read; permission view = viewer & read }
//
// Structure:
//   - 499 resources, each with viewer@user:tom and organization@organization:org1
//   - organization:org1 has reader@user:tom
//
// The view permission uses intersection (viewer & read), where read resolves
// through an arrow (organization->read) that itself uses exclusion (reader - banned).
// This tests the performance of intersection and exclusion in lookups.
func init() {
	registerBenchmark(Benchmark{
		Name:  "LookupIntersection",
		Tags:  []Tag{},
		Setup: setupLookupIntersection,
	})
}

func setupLookupIntersection(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	const numResources = 499

	schemaText := `
		definition user {}

		definition organization {
			relation reader: user
			relation banned: user
			permission read = reader - banned
		}

		definition resource {
			relation viewer: user
			relation organization: organization

			permission read = organization->read
			permission view = viewer & read
		}
	`

	_, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText)
	if err != nil {
		return nil, err
	}

	// 1 org reader + 2 per resource (viewer + organization)
	relationships := make([]tuple.Relationship, 0, 1+numResources*2)

	relationships = append(relationships, tuple.MustParse("organization:org1#reader@user:tom"))

	for i := 1; i <= numResources; i++ {
		relationships = append(relationships,
			tuple.MustParse(fmt.Sprintf("resource:resource%d#viewer@user:tom", i)),
			tuple.MustParse(fmt.Sprintf("resource:resource%d#organization@organization:org1", i)),
		)
	}

	_, err = writeRelationships(ctx, ds, relationships)
	if err != nil {
		return nil, err
	}

	expectedIDs := make([]string, numResources)
	for i := 1; i <= numResources; i++ {
		expectedIDs[i-1] = fmt.Sprintf("resource%d", i)
	}

	return &QuerySets{
		Checks: []CheckQuery{
			{
				ResourceType:    "resource",
				ResourceID:      "resource1",
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
				ExpectedResourceIDs: expectedIDs,
			},
		},
		IterSubjects: []IterSubjectsQuery{
			{
				ResourceType:       "resource",
				ResourceID:         "resource1",
				Permission:         "view",
				FilterSubjectType:  "user",
				ExpectedSubjectIDs: []string{"tom"},
			},
		},
	}, nil
}
