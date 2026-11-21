package datalayer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// watchViaStoredSchema watches the datastore, sourcing schema changes from the unified
// stored schema rather than from the legacy per-definition schema storage.
//
// The datastore only reports that the stored schema was written at a revision, so the
// definitions that changed are computed here, by comparing the stored schema at that
// revision against the stored schema as of the previously emitted change.
//
// This holds under the EmitImmediatelyStrategy as well: although changes to different rows
// can then arrive out of revision order, every write of the stored schema changes the same
// row, and changes to a single row are delivered in revision order.
func watchViaStoredSchema(
	ctx context.Context,
	ds datastore.Datastore,
	afterRevision datastore.Revision,
	opts datastore.WatchOptions,
) (<-chan datastore.RevisionChanges, <-chan error) {
	dsOpts := opts
	dsOpts.Content = (opts.Content &^ datastore.WatchSchema) | datastore.WatchStoredSchema

	dsUpdates, dsErrs := ds.Watch(ctx, afterRevision, dsOpts)

	updates := make(chan datastore.RevisionChanges, cap(dsUpdates))
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		// Load the schema as of the starting revision now, rather than when the first change
		// arrives, as the starting revision may have been garbage collected by then.
		previous, err := readStoredSchemaForWatch(ctx, ds, afterRevision)
		if err != nil {
			errs <- watchErrorFor(ctx, err)
			return
		}
		previousRevision := afterRevision

		for {
			select {
			case update, ok := <-dsUpdates:
				if !ok {
					// The datastore reports why the watch ended on its error channel.
					select {
					case err, ok := <-dsErrs:
						if ok && err != nil {
							errs <- err
						}
					case <-ctx.Done():
						errs <- datastore.NewWatchCanceledErr()
					}
					return
				}

				if update.StoredSchemaChanged {
					update.StoredSchemaChanged = false

					// A datastore may deliver a change more than once. A write that was already
					// compared must not be compared again, as it would be against a later schema.
					if update.Revision.GreaterThan(previousRevision) {
						current, err := readStoredSchemaForWatch(ctx, ds, update.Revision)
						if err != nil {
							errs <- watchErrorFor(ctx, err)
							return
						}

						addStoredSchemaChanges(&update, previous, current)
						previous = current
						previousRevision = update.Revision
					}

					// The stored schema is rewritten in full on every schema write, even if
					// nothing in it changed.
					if isEmptyRevisionChanges(update) {
						continue
					}
				}

				select {
				case updates <- update:
				case <-ctx.Done():
					errs <- datastore.NewWatchCanceledErr()
					return
				}

			case err, ok := <-dsErrs:
				if ok && err != nil {
					errs <- err
				}
				return
			}
		}
	}()

	return updates, errs
}

func watchErrorFor(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return datastore.NewWatchCanceledErr()
	}
	return err
}

// readStoredSchemaForWatch reads the stored schema at the given revision, returning an
// empty schema if none has been written.
func readStoredSchemaForWatch(ctx context.Context, ds datastore.Datastore, rev datastore.Revision) (*core.StoredSchema_V1StoredSchema, error) {
	stored, err := ds.SnapshotReader(rev).ReadStoredSchema(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			return &core.StoredSchema_V1StoredSchema{}, nil
		}
		return nil, fmt.Errorf("failed to read stored schema for watch: %w", err)
	}

	if v1 := stored.Get().GetV1(); v1 != nil {
		return v1, nil
	}
	return &core.StoredSchema_V1StoredSchema{}, nil
}

// addStoredSchemaChanges adds to the update the definitions that were added, changed or
// removed between the previous and current stored schemas.
func addStoredSchemaChanges(update *datastore.RevisionChanges, previous, current *core.StoredSchema_V1StoredSchema) {
	if current.SchemaHash != "" && current.SchemaHash == previous.SchemaHash {
		return
	}

	for _, name := range slices.Sorted(maps.Keys(current.NamespaceDefinitions)) {
		def := current.NamespaceDefinitions[name]
		if existing, ok := previous.NamespaceDefinitions[name]; !ok || !def.EqualVT(existing) {
			update.ChangedDefinitions = append(update.ChangedDefinitions, def)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(current.CaveatDefinitions)) {
		def := current.CaveatDefinitions[name]
		if existing, ok := previous.CaveatDefinitions[name]; !ok || !def.EqualVT(existing) {
			update.ChangedDefinitions = append(update.ChangedDefinitions, def)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(previous.NamespaceDefinitions)) {
		if _, ok := current.NamespaceDefinitions[name]; !ok {
			update.DeletedNamespaces = append(update.DeletedNamespaces, name)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(previous.CaveatDefinitions)) {
		if _, ok := current.CaveatDefinitions[name]; !ok {
			update.DeletedCaveats = append(update.DeletedCaveats, name)
		}
	}
}

func isEmptyRevisionChanges(update datastore.RevisionChanges) bool {
	return !update.IsCheckpoint &&
		len(update.RelationshipChanges) == 0 &&
		len(update.ChangedDefinitions) == 0 &&
		len(update.DeletedNamespaces) == 0 &&
		len(update.DeletedCaveats) == 0 &&
		len(update.Metadatas) == 0
}
