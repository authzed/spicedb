package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
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

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("benchmark"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	if err != nil {
		return nil, err
	}

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	if err != nil {
		return nil, err
	}

	relationships := make([]tuple.Relationship, 0, numUsers+1)
	for i := 0; i < numUsers; i++ {
		relationships = append(relationships, tuple.MustParse(
			fmt.Sprintf("resource:someresource#viewer@user:user-%d", i)))
	}
	relationships = append(relationships, tuple.MustParse("resource:someresource#viewer@user:tom"))

	_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, relationships...)
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
