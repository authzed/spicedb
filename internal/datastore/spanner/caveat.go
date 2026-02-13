package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func (sr spannerReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	caveatKey := spanner.Key{name}
	row, err := sr.txSource().ReadRow(ctx, tableCaveat, caveatKey, []string{colCaveatDefinition, colCaveatTS})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return nil, datastore.NoRevision, datastore.NewCaveatNameNotFoundErr(name)
		}
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadCaveat, err)
	}
	var serialized []byte
	var updated time.Time
	if err := row.Columns(&serialized, &updated); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadCaveat, err)
	}

	loaded := &core.CaveatDefinition{}
	if err := loaded.UnmarshalVT(serialized); err != nil {
		return nil, datastore.NoRevision, err
	}
	return loaded, revisions.NewForTime(updated), nil
}

func (sr spannerReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return sr.listCaveats(ctx, nil)
}

func (sr spannerReader) LegacyLookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	if len(caveatNames) == 0 {
		return nil, nil
	}
	return sr.listCaveats(ctx, caveatNames)
}

func (sr spannerReader) listCaveats(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	keyset := spanner.AllKeys()
	if len(caveatNames) > 0 {
		keys := make([]spanner.Key, 0, len(caveatNames))
		for _, n := range caveatNames {
			keys = append(keys, spanner.Key{n})
		}
		keyset = spanner.KeySetFromKeys(keys...)
	}
	iter := sr.txSource().Read(
		ctx,
		tableCaveat,
		keyset,
		[]string{colCaveatDefinition, colCaveatTS},
	)
	defer iter.Stop()

	var caveats []datastore.RevisionedCaveat
	if err := iter.Do(func(row *spanner.Row) error {
		var serialized []byte
		var updated time.Time
		if err := row.Columns(&serialized, &updated); err != nil {
			return err
		}

		loaded := &core.CaveatDefinition{}
		if err := loaded.UnmarshalVT(serialized); err != nil {
			return err
		}
		caveats = append(caveats, datastore.RevisionedCaveat{
			Definition:          loaded,
			LastWrittenRevision: revisions.NewForTime(updated),
		})

		return nil
	}); err != nil {
		return nil, fmt.Errorf(errUnableToListCaveats, err)
	}

	return caveats, nil
}

func (rwt spannerReadWriteTXN) LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	names := map[string]struct{}{}
	mutations := make([]*spanner.Mutation, 0, len(caveats))
	for _, caveat := range caveats {
		if _, ok := names[caveat.Name]; ok {
			return fmt.Errorf(errUnableToWriteCaveat, fmt.Errorf("duplicate caveats in input: %s", caveat.Name))
		}
		names[caveat.Name] = struct{}{}
		serialized, err := caveat.MarshalVT()
		if err != nil {
			return fmt.Errorf(errUnableToWriteCaveat, err)
		}

		mutations = append(mutations, spanner.InsertOrUpdate(
			tableCaveat,
			[]string{colName, colCaveatDefinition, colCaveatTS},
			[]any{caveat.Name, serialized, spanner.CommitTimestamp},
		))

		// Track the buffered caveat write so we can return it from List methods
		// without attempting to read from Spanner (which doesn't see buffered writes)
		rwt.bufferedCaveats[caveat.Name] = caveat
		// Remove from deleted set in case it was previously deleted in this transaction
		delete(rwt.deletedCaveats, caveat.Name)
	}

	if err := rwt.spannerRWT.BufferWrite(mutations); err != nil {
		return err
	}

	return nil
}

func (rwt spannerReadWriteTXN) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	keys := make([]spanner.Key, 0, len(names))
	for _, n := range names {
		keys = append(keys, spanner.Key{n})
		// Remove from buffered caveats and mark as deleted so List methods won't return it
		delete(rwt.bufferedCaveats, n)
		rwt.deletedCaveats[n] = struct{}{}
	}
	err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
		spanner.Delete(tableCaveat, spanner.KeySetFromKeys(keys...)),
	})
	if err != nil {
		return fmt.Errorf(errUnableToDeleteCaveat, err)
	}

	return nil
}

func ContextualizedCaveatFrom(name spanner.NullString, context spanner.NullJSON) (*core.ContextualizedCaveat, error) {
	if name.Valid && name.StringVal != "" {
		var cctx map[string]any
		if context.Valid && context.Value != nil {
			cctx = context.Value.(map[string]any)
		}
		return common.ContextualizedCaveatFrom(name.StringVal, cctx)
	}
	return nil, nil
}
