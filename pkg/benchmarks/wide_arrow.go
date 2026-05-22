package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// WideArrow benchmarks permission checking through a wide arrow relationship.
//
// Schema: user + group (member) + file (group, view, viewer = view + group->member)
//
//   - 10 files, each belonging to 30 groups (deterministic stepping assignment)
//   - 97 groups (prime), each with 20 users (deterministic stepping assignment)
//   - 997 users (prime)
//
// Checking if a user has viewer permission on a file requires checking many
// group memberships via the group->member arrow.
func init() {
	registerBenchmark(Benchmark{
		Name:  "WideArrow",
		Tags:  []Tag{Arrows},
		Setup: setupWideArrow,
	})
}

func setupWideArrow(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	const (
		numFiles      = 10
		numGroups     = 97  // prime
		numUsers      = 997 // prime
		groupsPerFile = 30
		usersPerGroup = 20
	)

	schemaText := `
		definition user {}

		definition group {
			relation member: user
		}

		definition file {
			relation group: group
			relation view: user
			permission viewer = view + group->member
		}
	`

	_, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText)
	if err != nil {
		return nil, err
	}

	relationships := make([]tuple.Relationship, 0, numFiles*groupsPerFile+numGroups*usersPerGroup)

	for fileID := 0; fileID < numFiles; fileID++ {
		step := fileID + 1
		for i := 0; i < groupsPerFile; i++ {
			groupID := (i * step) % numGroups
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("file:file%d#group@group:group%d", fileID, groupID)))
		}
	}

	for groupID := 0; groupID < numGroups; groupID++ {
		step := groupID + 1
		for i := 0; i < usersPerGroup; i++ {
			userID := (i * step) % numUsers
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("group:group%d#member@user:user%d", groupID, userID)))
		}
	}

	_, err = writeRelationships(ctx, ds, relationships)
	if err != nil {
		return nil, err
	}

	return &QuerySets{
		Checks: []CheckQuery{
			{
				ResourceType:    "file",
				ResourceID:      "file0",
				Permission:      "viewer",
				SubjectType:     "user",
				SubjectID:       "user570",
				SubjectRelation: tuple.Ellipsis,
			},
		},
	}, nil
}
