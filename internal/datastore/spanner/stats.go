package spanner

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	queryRelationshipEstimate = fmt.Sprintf("SELECT SUM(%s) FROM %s", colCount, tableCounters)

	rng = rand.NewSource(time.Now().UnixNano())
)

func (sd spannerDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	var uniqueID string
	if err := sd.client.Single().Read(
		context.Background(),
		tableMetadata,
		spanner.AllKeys(),
		[]string{colUniqueID},
	).Do(func(r *spanner.Row) error {
		return r.Columns(&uniqueID)
	}); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read unique ID: %w", err)
	}

	iter := sd.client.Single().Read(
		ctx,
		tableNamespace,
		spanner.AllKeys(),
		[]string{colNamespaceConfig, colNamespaceTS},
	)

	allNamespaces, err := readAllNamespaces(iter)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read namespaces: %w", err)
	}

	var estimate spanner.NullInt64
	if err := sd.client.Single().Query(ctx, spanner.Statement{SQL: queryRelationshipEstimate}).Do(func(r *spanner.Row) error {
		return r.Columns(&estimate)
	}); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read row counts: %w", err)
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(allNamespaces),
		EstimatedRelationshipCount: uint64(estimate.Int64),
	}, nil
}

func updateCounter(ctx context.Context, rwt *spanner.ReadWriteTransaction, change int64) error {
	newValue := change

	counterID := make([]byte, 2)
	_, err := rand.New(rng).Read(counterID)
	if err != nil {
		return fmt.Errorf("unable to select random counter: %w", err)
	}

	counterRow, err := rwt.ReadRow(ctx, tableCounters, spanner.Key{counterID}, []string{colCount})
	if err != nil {
		if spanner.ErrCode(err) != codes.NotFound {
			return fmt.Errorf("unable to read counter value: %w", err)
		}
		// In this branch we leave newValue alone because the counter doesn't exist
	} else {
		var currentValue int64
		if err := counterRow.Columns(&currentValue); err != nil {
			return fmt.Errorf("unable to decode counter value: %w", err)
		}
		newValue += currentValue
	}

	log.Ctx(ctx).Trace().
		Bytes("counterID", counterID).
		Int64("newValue", newValue).
		Int64("change", change).
		Msg("updating counter")

	if err := rwt.BufferWrite([]*spanner.Mutation{
		spanner.InsertOrUpdate(tableCounters, []string{colID, colCount}, []interface{}{counterID, newValue}),
	}); err != nil {
		return fmt.Errorf("unable to buffer update to counter: %w", err)
	}

	return nil
}
