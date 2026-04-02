package benchmarks

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DeepArrow benchmarks permission checking through a deep recursive chain.
//
// Schema: user + document (parent, view, viewer = view + parent->viewer)
//
// A 30+ level deep parent chain: document:target -> document:1 -> ... -> document:30,
// with document:29#view@user:slow. Checking if user:slow has viewer permission on
// document:target requires recursive traversal through all 30+ levels.
func init() {
	registerBenchmark(Benchmark{
		Name:  "DeepArrow",
		Tags:  []Tag{Recursion},
		Setup: setupDeepArrow,
	})
}

func setupDeepArrow(ctx context.Context, ds datastore.Datastore) (*QuerySets, error) {
	schemaText := `
		definition user {}

		definition document {
			relation parent: document
			relation view: user
			permission viewer = view + parent->viewer
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

	// Chain: document:target -> document:1 -> document:2 -> ... -> document:30
	// Plus: document:29#view@user:slow
	relationships := make([]tuple.Relationship, 0, 33)
	relationships = append(relationships, tuple.MustParse("document:target#parent@document:1"))
	for i := 1; i <= 30; i++ {
		relationships = append(relationships, tuple.MustParse(
			fmt.Sprintf("document:%d#parent@document:%d", i, i+1)))
	}
	relationships = append(relationships, tuple.MustParse("document:29#view@user:slow"))

	_, err = writeRelationships(ctx, ds, relationships)
	if err != nil {
		return nil, err
	}

	return &QuerySets{
		Checks: []CheckQuery{
			{
				ResourceType:    "document",
				ResourceID:      "target",
				Permission:      "viewer",
				SubjectType:     "user",
				SubjectID:       "slow",
				SubjectRelation: tuple.Ellipsis,
			},
		},
		MaxRecursionDepth: 50,
	}, nil
}
