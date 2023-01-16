package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
)

var (
	writeCaveat  = psql.Insert(tableCaveat).Columns(colCaveatName, colCaveatDefinition)
	listCaveat   = psql.Select(colCaveatDefinition).From(tableCaveat).OrderBy(colCaveatName)
	readCaveat   = psql.Select(colCaveatDefinition, colCreatedXid).From(tableCaveat)
	deleteCaveat = psql.Update(tableCaveat).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
)

const (
	errWriteCaveats  = "unable to write caveats: %w"
	errDeleteCaveats = "unable delete caveats: %w"
	errListCaveats   = "unable to list caveats: %w"
	errReadCaveat    = "unable to read caveat: %w"
)

func (r *pgReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	filteredReadCaveat := r.filterer(readCaveat)
	sql, args, err := filteredReadCaveat.Where(sq.Eq{colCaveatName: name}).ToSql()
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}

	tx, txCleanup, err := r.txSource(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}
	defer txCleanup(ctx)

	var txID xid8
	var serializedDef []byte
	err = tx.QueryRow(ctx, sql, args...).Scan(&serializedDef, &txID)
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
	rev := postgresRevision{txID, noXmin}

	return &def, rev, nil
}

func (r *pgReader) ListCaveats(ctx context.Context, caveatNames ...string) ([]*core.CaveatDefinition, error) {
	caveatsWithNames := listCaveat
	if len(caveatNames) > 0 {
		caveatsWithNames = caveatsWithNames.Where(sq.Eq{colCaveatName: caveatNames})
	}

	filteredListCaveat := r.filterer(caveatsWithNames)
	sql, args, err := filteredListCaveat.ToSql()
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}

	tx, txCleanup, err := r.txSource(ctx)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}
	defer txCleanup(ctx)

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}

	defer rows.Close()
	var caveats []*core.CaveatDefinition
	for rows.Next() {
		var defBytes []byte
		err = rows.Scan(&defBytes)
		if err != nil {
			return nil, fmt.Errorf(errListCaveats, err)
		}
		c := core.CaveatDefinition{}
		err = c.UnmarshalVT(defBytes)
		if err != nil {
			return nil, fmt.Errorf(errListCaveats, err)
		}
		caveats = append(caveats, &c)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf(errListCaveats, rows.Err())
	}

	return caveats, nil
}

func (rwt *pgReadWriteTXN) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	if len(caveats) == 0 {
		return nil
	}
	write := writeCaveat
	writtenCaveatNames := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		definitionBytes, err := caveat.MarshalVT()
		if err != nil {
			return fmt.Errorf(errWriteCaveats, err)
		}
		valuesToWrite := []any{caveat.Name, definitionBytes}
		write = write.Values(valuesToWrite...)
		writtenCaveatNames = append(writtenCaveatNames, caveat.Name)
	}

	// mark current caveats as deleted
	err := rwt.deleteCaveatsFromNames(ctx, writtenCaveatNames)
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
		Set(colDeletedXid, rwt.newXID).
		Where(sq.Eq{colCaveatName: names}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errDeleteCaveats, err)
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errDeleteCaveats, err)
	}
	return nil
}
