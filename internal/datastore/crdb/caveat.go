package crdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var (
	upsertCaveatSuffix = fmt.Sprintf(
		"ON CONFLICT (%s) DO UPDATE SET %s = excluded.%s",
		schema.ColCaveatName,
		schema.ColCaveatDefinition,
		schema.ColCaveatDefinition,
	)
	writeCaveat  = psql.Insert(schema.TableCaveat).Columns(schema.ColCaveatName, schema.ColCaveatDefinition).Suffix(upsertCaveatSuffix)
	readCaveat   = psql.Select(schema.ColCaveatDefinition, schema.ColTimestamp)
	listCaveat   = psql.Select(schema.ColCaveatName, schema.ColCaveatDefinition, schema.ColTimestamp).OrderBy(schema.ColCaveatName)
	deleteCaveat = psql.Delete(schema.TableCaveat)
)

const (
	errWriteCaveat   = "unable to write new caveat revision: %w"
	errReadCaveat    = "unable to read new caveat `%s`: %w"
	errListCaveats   = "unable to list caveat: %w"
	errDeleteCaveats = "unable to delete caveats: %w"
)

func (cr *crdbReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	query := cr.addFromToQuery(readCaveat.Where(sq.Eq{schema.ColCaveatName: name}), schema.TableCaveat)
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, name, err)
	}
	cr.assertHasExpectedAsOfSystemTime(sql)

	var definitionBytes []byte
	var timestamp time.Time

	err = cr.query.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&definitionBytes, &timestamp)
	}, sql, args...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = datastore.NewCaveatNameNotFoundErr(name)
		}
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, name, err)
	}

	loaded := &core.CaveatDefinition{}
	if err := loaded.UnmarshalVT(definitionBytes); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, name, err)
	}
	cr.addOverlapKey(name)
	return loaded, revisions.NewHLCForTime(timestamp), nil
}

func (cr *crdbReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	if len(caveatNames) == 0 {
		return nil, nil
	}
	return cr.lookupCaveats(ctx, caveatNames)
}

func (cr *crdbReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return cr.lookupCaveats(ctx, nil)
}

type bytesAndTimestamp struct {
	bytes     []byte
	timestamp time.Time
}

func (cr *crdbReader) lookupCaveats(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	caveatsWithNames := cr.addFromToQuery(listCaveat, schema.TableCaveat)
	if len(caveatNames) > 0 {
		caveatsWithNames = caveatsWithNames.Where(sq.Eq{schema.ColCaveatName: caveatNames})
	}

	sql, args, err := caveatsWithNames.ToSql()
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}
	cr.assertHasExpectedAsOfSystemTime(sql)

	var allDefinitionBytes []bytesAndTimestamp

	err = cr.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var defBytes []byte
			var name string
			var timestamp time.Time
			err = rows.Scan(&name, &defBytes, &timestamp)
			if err != nil {
				return fmt.Errorf(errListCaveats, err)
			}
			allDefinitionBytes = append(allDefinitionBytes, bytesAndTimestamp{bytes: defBytes, timestamp: timestamp})
			cr.addOverlapKey(name)
		}
		return nil
	}, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}

	caveats := make([]datastore.RevisionedCaveat, 0, len(allDefinitionBytes))
	for _, bat := range allDefinitionBytes {
		loaded := &core.CaveatDefinition{}
		if err := loaded.UnmarshalVT(bat.bytes); err != nil {
			return nil, fmt.Errorf(errListCaveats, err)
		}
		caveats = append(caveats, datastore.RevisionedCaveat{
			Definition:          loaded,
			LastWrittenRevision: revisions.NewHLCForTime(bat.timestamp),
		})
	}

	return caveats, nil
}

func (rwt *crdbReadWriteTXN) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	if len(caveats) == 0 {
		return nil
	}
	write := writeCaveat
	writtenCaveatNames := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		definitionBytes, err := caveat.MarshalVT()
		if err != nil {
			return fmt.Errorf(errWriteCaveat, err)
		}
		valuesToWrite := []any{caveat.Name, definitionBytes}
		write = write.Values(valuesToWrite...)
		writtenCaveatNames = append(writtenCaveatNames, caveat.Name)
	}

	// store the new caveat
	sql, args, err := write.ToSql()
	if err != nil {
		return fmt.Errorf(errWriteCaveat, err)
	}

	for _, val := range writtenCaveatNames {
		rwt.addOverlapKey(val)
	}
	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errWriteCaveat, err)
	}
	return nil
}

func (rwt *crdbReadWriteTXN) DeleteCaveats(ctx context.Context, names []string) error {
	deleteCaveatClause := deleteCaveat.Where(sq.Eq{schema.ColCaveatName: names})
	sql, args, err := deleteCaveatClause.ToSql()
	if err != nil {
		return fmt.Errorf(errDeleteCaveats, err)
	}
	for _, val := range names {
		rwt.addOverlapKey(val)
	}
	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errDeleteCaveats, err)
	}
	return nil
}
