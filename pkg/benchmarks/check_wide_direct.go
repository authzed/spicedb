package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// CheckWideDirect benchmarks checking a permission on a resource that has many
// direct user relationships (9000 users + the target user).
//
// Schema: user + resource (viewer, view = viewer)
//
// This tests the performance of direct relationship checks when the relation
// set is very wide.
func init() {
	registerBenchmark(Benchmark{
		Name:  "CheckWideDirect",
		Tags:  []Tag{},
		Setup: setupCheckWideDirect,
	})
}

func setupCheckWideDirect(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	const numUsers = 9000

	schemaText := `
		definition user {}

		definition resource {
			relation viewer: user
			permission view = viewer
		}
	`

	_, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText)
	if err != nil {
		return nil, err
	}

	relationships := make([]tuple.Relationship, 0, numUsers+1)
	for i := 0; i < numUsers; i++ {
		relationships = append(relationships, tuple.MustParse(
			fmt.Sprintf("resource:someresource#viewer@user:user-%d", i)))
	}
	relationships = append(relationships, tuple.MustParse("resource:someresource#viewer@user:tom"))

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
	}, nil
}
