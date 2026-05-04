package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DoubleWideArrow benchmarks permission checking through two consecutive arrow hops
// with wide fan-out at each level.
//
// Schema: user + group (member) + org (group, member = group->member) +
//
//	file (org, view, viewer = view + org->member)
//
// Hierarchy: file -> org -> group -> user
//   - 5 files, each belonging to 20 orgs
//   - 97 orgs (prime), each containing 10 groups
//   - 299 groups (prime), each with 20 members
//   - 499 users (prime)
//
// Checking viewer on a file requires two arrow traversals:
//  1. file->org  (fanout: orgs per file)
//  2. org->member which resolves to org->group->member (double fan-out)
func init() {
	registerBenchmark(Benchmark{
		Name:  "DoubleWideArrow",
		Tags:  []Tag{Arrows},
		Setup: setupDoubleWideArrow,
	})
}

func setupDoubleWideArrow(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	const (
		numFiles      = 5
		numOrgs       = 97  // prime
		numGroups     = 299 // prime
		numUsers      = 499 // prime
		orgsPerFile   = 20
		groupsPerOrg  = 10
		usersPerGroup = 20
	)

	schemaText := `
		definition user {}

		definition group {
			relation member: user
		}

		definition org {
			relation group: group
			permission member = group->member
		}

		definition file {
			relation org: org
			relation view: user
			permission viewer = view + org->member
		}
	`

	_, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText)
	if err != nil {
		return nil, err
	}

	relationships := make([]tuple.Relationship, 0,
		numFiles*orgsPerFile+numOrgs*groupsPerOrg+numGroups*usersPerGroup)

	for fileID := 0; fileID < numFiles; fileID++ {
		step := fileID + 1
		for i := 0; i < orgsPerFile; i++ {
			orgID := (i * step) % numOrgs
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("file:file%d#org@org:org%d", fileID, orgID)))
		}
	}

	for orgID := 0; orgID < numOrgs; orgID++ {
		step := orgID + 1
		for i := 0; i < groupsPerOrg; i++ {
			groupID := (i * step) % numGroups
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("org:org%d#group@group:group%d", orgID, groupID)))
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
				SubjectID:       "user181",
				SubjectRelation: tuple.Ellipsis,
			},
		},
	}, nil
}
