package tidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errDeleteCaveat = "unable to delete caveats: %w"
	errReadCaveat   = "unable to read caveat: %w"
	errListCaveats  = "unable to list caveats: %w"
	errWriteCaveats = "unable to write caveats: %w"
)

func (tr *tidbReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	filteredReadCaveat := tr.aliveFilter(tr.ReadCaveatQuery)
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
	var txID uint64
	err = tx.QueryRowContext(ctx, sqlStatement, args...).Scan(&serializedDef, &txID)
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
	return &def, revisions.NewForTransactionID(txID), nil
}

func (tr *tidbReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	if len(caveatNames) == 0 {
		return nil, nil
	}
	return tr.lookupCaveats(ctx, caveatNames)
}

func (tr *tidbReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return tr.lookupCaveats(ctx, nil)
}

func (tr *tidbReader) lookupCaveats(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	caveatsWithNames := tr.ListCaveatsQuery
	if len(caveatNames) > 0 {
		caveatsWithNames = caveatsWithNames.Where(sq.Eq{colName: caveatNames})
	}

	filteredListCaveat := tr.aliveFilter(caveatsWithNames)
	listSQL, listArgs, err := filteredListCaveat.ToSql()
	if err != nil {
		return nil, err
	}

	tx, txCleanup, err := tr.txSource(ctx)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}
	defer common.LogOnError(ctx, txCleanup)

	rows, err := tx.QueryContext(ctx, listSQL, listArgs...)
	if err != nil {
		return nil, fmt.Errorf(errListCaveats, err)
	}
	defer common.LogOnError(ctx, rows.Close)

	var caveats []datastore.RevisionedCaveat
	for rows.Next() {
		var defBytes []byte
		var txID uint64

		err = rows.Scan(&defBytes, &txID)
		if err != nil {
			return nil, fmt.Errorf(errListCaveats, err)
		}
		c := core.CaveatDefinition{}
		err = c.UnmarshalVT(defBytes)
		if err != nil {
			return nil, fmt.Errorf(errListCaveats, err)
		}
		caveats = append(caveats, datastore.RevisionedCaveat{
			Definition:          &c,
			LastWrittenRevision: revisions.NewForTransactionID(txID),
		})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf(errListCaveats, rows.Err())
	}

	return caveats, nil
}

func (rwt *tidbReadWriteTXN) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
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

func (rwt *tidbReadWriteTXN) DeleteCaveats(ctx context.Context, names []string) error {
	return rwt.deleteCaveatsFromNames(ctx, names)
}

func (rwt *tidbReadWriteTXN) deleteCaveatsFromNames(ctx context.Context, names []string) error {
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
