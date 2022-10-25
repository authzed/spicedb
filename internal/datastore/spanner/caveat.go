package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func (sr spannerReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "ReadCaveatByName", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()

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
	return loaded, revisionFromTimestamp(updated), nil
}

func (sr spannerReader) ListCaveats(ctx context.Context, caveatNames ...string) ([]*core.CaveatDefinition, error) {
	ctx, span := tracer.Start(ctx, "ListCaveats", trace.WithAttributes(attribute.StringSlice("names", caveatNames)))
	defer span.End()

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
		[]string{colCaveatDefinition},
	)

	var caveats []*core.CaveatDefinition
	if err := iter.Do(func(row *spanner.Row) error {
		var serialized []byte
		if err := row.Columns(&serialized); err != nil {
			return err
		}

		loaded := &core.CaveatDefinition{}
		if err := loaded.UnmarshalVT(serialized); err != nil {
			return err
		}
		caveats = append(caveats, loaded)

		return nil
	}); err != nil {
		return nil, fmt.Errorf(errUnableToListCaveats, err)
	}

	return caveats, nil
}

func (rwt spannerReadWriteTXN) WriteCaveats(caveats []*core.CaveatDefinition) error {
	_, span := tracer.Start(rwt.ctx, "WriteCaveats")
	defer span.End()

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
			[]interface{}{caveat.Name, serialized, spanner.CommitTimestamp},
		))
	}

	return rwt.spannerRWT.BufferWrite(mutations)
}

func (rwt spannerReadWriteTXN) DeleteCaveats(names []string) error {
	_, span := tracer.Start(rwt.ctx, "DeleteCaveats", trace.WithAttributes(attribute.StringSlice("names", names)))
	defer span.End()

	keys := make([]spanner.Key, 0, len(names))
	for _, n := range names {
		keys = append(keys, spanner.Key{n})
	}
	err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
		spanner.Delete(tableCaveat, spanner.KeySetFromKeys(keys...)),
	})
	if err != nil {
		return fmt.Errorf(errUnableToDeleteCaveat, err)
	}

	return err
}

func ContextualizedCaveatFrom(name spanner.NullString, context spanner.NullJSON) (*core.ContextualizedCaveat, error) {
	if name.Valid {
		var cctx map[string]any
		if context.Valid {
			cctx = context.Value.(map[string]any)
		}
		return common.ContextualizedCaveatFrom(name.StringVal, cctx)
	}
	return nil, nil
}
