package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	sq "github.com/Masterminds/squirrel"
	"github.com/shopspring/decimal"
)

const (
	errDeleteCaveat = "unable to delete caveats: %w"
	errReadCaveat   = "unable to read caveat: %w"
	errListCaveats  = "unable to list caveats: %w"
	errWriteCaveats = "unable to write caveats: %w"
)

func (mr *mysqlReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	filteredReadCaveat := mr.filterer(mr.ReadCaveatQuery)
	sqlStatement, args, err := filteredReadCaveat.Where(sq.Eq{colName: name}).ToSql()
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}
	defer common.LogOnError(ctx, txCleanup)

	var serializedDef []byte
	var rev decimal.Decimal
	err = tx.QueryRowContext(ctx, sqlStatement, args...).Scan(&serializedDef, &rev)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, datastore.NoRevision, datastore.NewCaveatNameNotFoundErr(name)
		}
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}
	def := core.CaveatDefinition{}
	err = def.UnmarshalVT(serializedDef)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errReadCaveat, err)
	}
	return &def, revision.NewFromDecimal(rev), nil
}

func (mr *mysqlReader) ListCaveats(ctx context.Context, caveatNames ...string) ([]*core.CaveatDefinition, error) {
	caveatsWithNames := mr.ListCaveatsQuery
	if len(caveatNames) > 0 {
		caveatsWithNames = caveatsWithNames.Where(sq.Eq{colName: caveatNames})
	}

	filteredListCaveat := mr.filterer(caveatsWithNames)
	listSQL, listArgs, err := filteredListCaveat.ToSql()
	if err != nil {
		return nil, err
	}

	tx, txCleanup, err := mr.txSource(ctx)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}
	defer common.LogOnError(ctx, txCleanup)

	rows, err := tx.QueryContext(ctx, listSQL, listArgs...)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}
	defer common.LogOnError(ctx, rows.Close)

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

func (rwt *mysqlReadWriteTXN) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	if len(caveats) == 0 {
		return nil
	}
	writeQuery := rwt.WriteCaveatQuery

	caveatNamesToWrite := make([]string, 0, len(caveats))
	for _, newCaveat := range caveats {
		serialized, err := newCaveat.MarshalVT()
		if err != nil {
			return fmt.Errorf("unable to write caveat: %w", err)
		}

		writeQuery = writeQuery.Values(newCaveat.Name, serialized, rwt.newTxnID)
		caveatNamesToWrite = append(caveatNamesToWrite, newCaveat.Name)
	}

	err := rwt.deleteCaveatsFromNames(ctx, caveatNamesToWrite)
	if err != nil {
		return fmt.Errorf(errWriteCaveats, err)
	}

	querySQL, writeArgs, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errWriteCaveats, err)
	}

	_, err = rwt.tx.ExecContext(ctx, querySQL, writeArgs...)
	if err != nil {
		return fmt.Errorf(errWriteCaveats, err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) DeleteCaveats(ctx context.Context, names []string) error {
	return rwt.deleteCaveatsFromNames(ctx, names)
}

func (rwt *mysqlReadWriteTXN) deleteCaveatsFromNames(ctx context.Context, names []string) error {
	delSQL, delArgs, err := rwt.DeleteCaveatQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Eq{colName: names}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errDeleteCaveat, err)
	}

	_, err = rwt.tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errDeleteCaveat, err)
	}
	return nil
}
