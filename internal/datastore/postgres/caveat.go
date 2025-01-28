package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	sq "github.com/Masterminds/squirrel"
)

var (
	writeCaveat = psql.Insert(schema.TableCaveat).Columns(schema.ColCaveatName, schema.ColCaveatDefinition)
	listCaveat  = psql.
			Select(schema.ColCaveatDefinition, schema.ColCreatedXid).
			From(schema.TableCaveat).
			OrderBy(schema.ColCaveatName)
	readCaveat = psql.
			Select(schema.ColCaveatDefinition, schema.ColCreatedXid).
			From(schema.TableCaveat)
	deleteCaveat = psql.Update(schema.TableCaveat).Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})
)

const (
	errWriteCaveats  = "unable to write caveats: %w"
	errDeleteCaveats = "unable delete caveats: %w"
	errListCaveats   = "unable to list caveats: %w"
	errReadCaveat    = "unable to read caveat: %w"
)

func (r *pgReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	filteredReadCaveat := r.aliveFilter(readCaveat)
	sql, args, err := filteredReadCaveat.Where(sq.Eq{schema.ColCaveatName: name}).ToSql()
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}

	var txID xid8
	var serializedDef []byte
	err = r.query.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&serializedDef, &txID)
	}, sql, args...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, datastore.NoRevision, datastore.NewCaveatNameNotFoundErr(name)
		}
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}
	def := core.CaveatDefinition{}
	err = def.UnmarshalVT(serializedDef)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}

	rev := revisionForVersion(txID)

	return &def, rev, nil
}

func (r *pgReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	if len(caveatNames) == 0 {
		return nil, nil
	}
	return r.lookupCaveats(ctx, caveatNames)
}

func (r *pgReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return r.lookupCaveats(ctx, nil)
}

func (r *pgReader) lookupCaveats(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	caveatsWithNames := listCaveat
	if len(caveatNames) > 0 {
		caveatsWithNames = caveatsWithNames.Where(sq.Eq{schema.ColCaveatName: caveatNames})
	}

	filteredListCaveat := r.aliveFilter(caveatsWithNames)
	sql, args, err := filteredListCaveat.ToSql()
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}

	var caveats []datastore.RevisionedCaveat
	err = r.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var version xid8
			var defBytes []byte
			err = rows.Scan(&defBytes, &version)
			if err != nil {
				return fmt.Errorf(errListCaveats, err)
			}
			c := core.CaveatDefinition{}
			err = c.UnmarshalVT(defBytes)
			if err != nil {
				return fmt.Errorf(errListCaveats, err)
			}

			revision := revisionForVersion(version)
			caveats = append(caveats, datastore.RevisionedCaveat{Definition: &c, LastWrittenRevision: revision})
		}
		return rows.Err()
	}, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}

	return caveats, nil
}

func (rwt *pgReadWriteTXN) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	if len(caveats) == 0 {
		return nil
	}
	write := writeCaveat
	writtenCaveatNames := mapz.NewSet[string]()
	for _, caveat := range caveats {
		definitionBytes, err := caveat.MarshalVT()
		if err != nil {
			return fmt.Errorf(errWriteCaveats, err)
		}
		valuesToWrite := []any{caveat.Name, definitionBytes}
		write = write.Values(valuesToWrite...)
		if !writtenCaveatNames.Add(caveat.Name) {
			return fmt.Errorf("duplicate caveat name %q", caveat.Name)
		}
	}

	// mark current caveats as deleted
	err := rwt.deleteCaveatsFromNames(ctx, writtenCaveatNames.AsSlice())
	if err != nil {
		return fmt.Errorf(errWriteCaveats, err)
	}

	// store the new caveat revision
	sql, args, err := write.ToSql()
	if err != nil {
		return fmt.Errorf(errWriteCaveats, err)
	}
	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errWriteCaveats, err)
	}
	return nil
}

func (rwt *pgReadWriteTXN) DeleteCaveats(ctx context.Context, names []string) error {
	// mark current caveats as deleted
	return rwt.deleteCaveatsFromNames(ctx, names)
}

func (rwt *pgReadWriteTXN) deleteCaveatsFromNames(ctx context.Context, names []string) error {
	sql, args, err := deleteCaveat.
		Set(schema.ColDeletedXid, rwt.newXID).
		Where(sq.Eq{schema.ColCaveatName: names}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errDeleteCaveats, err)
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errDeleteCaveats, err)
	}
	return nil
}
