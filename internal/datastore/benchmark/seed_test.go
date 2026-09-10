package benchmark

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// seedChunkSize is the number of relationships written per transaction by
// SeedRelationships. Seeding in chunks spreads the rows over many transactions,
// so the revision columns hold a range of values as they would in a real
// deployment rather than a single one.
const seedChunkSize = 50_000

// RelationshipGeneratorFunc returns the relationship to write at the given
// index, which counts from zero across the whole seed.
type RelationshipGeneratorFunc func(index int) tuple.Relationship

// seedRelationships writes count generated relationships into ds.
//
// It uses the datastore's bulk-load API, so it works against any engine and is
// considerably faster than writing the same rows through WriteRelationships.
// Bulk loading skips the checks that WriteRelationships performs, which lets
// benchmarks seed resource and subject types that are not in the written
// schema; do not use it where those checks are what is being exercised.
func seedRelationships(ctx context.Context, tb testing.TB, ds datastore.Datastore, count int, generator RelationshipGeneratorFunc) {
	tb.Helper()

	for start := 0; start < count; start += seedChunkSize {
		remaining := min(seedChunkSize, count-start)

		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			loaded, err := rwt.BulkLoad(ctx, &generatedRelationshipSource{
				remaining: remaining,
				index:     start,
				generator: generator,
			})
			if err != nil {
				return err
			}

			require.EqualValues(tb, remaining, loaded)
			return nil
		})
		require.NoError(tb, err, "seeding relationships %d through %d", start, start+remaining)
	}
}

// generatedRelationshipSource adapts a RelationshipGeneratorFunc to the
// datastore's bulk write source interface.
type generatedRelationshipSource struct {
	generator RelationshipGeneratorFunc
	current   tuple.Relationship
	remaining int
	index     int
}

var _ datastore.BulkWriteRelationshipSource = (*generatedRelationshipSource)(nil)

func (s *generatedRelationshipSource) Next(_ context.Context) (*tuple.Relationship, error) {
	if s.remaining <= 0 {
		return nil, nil
	}

	s.remaining--
	s.current = s.generator(s.index)
	s.index++

	return &s.current, nil
}
