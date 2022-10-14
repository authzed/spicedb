package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	writeCaveat            = psql.Insert(tableCaveat).Columns(colCaveatName, colCaveatExpression)
	writeCaveatDeprecated  = psql.Insert(tableCaveat).Columns(colCaveatName, colCaveatExpression, colCreatedTxnDeprecated)
	readCaveat             = psql.Select(colCaveatExpression).From(tableCaveat)
	deleteCaveat           = psql.Update(tableCaveat).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
	deleteCaveatDeprecated = psql.Update(tableCaveat).Where(sq.Eq{colDeletedTxnDeprecated: liveDeletedTxnID})
)

func (r *pgReader) ReadCaveatByName(ctx context.Context, name string) (*core.Caveat, error) {
	ctx, span := tracer.Start(ctx, "ReadCaveatByName", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()

	filteredReadCaveat := r.filterer(readCaveat)
	sql, args, err := filteredReadCaveat.Where(sq.Eq{colCaveatName: name}).ToSql()
	if err != nil {
		return nil, err
	}

	tx, txCleanup, err := r.txSource(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to read caveat: %w", err)
	}
	defer txCleanup(ctx)

	var expr []byte
	err = tx.QueryRow(ctx, sql, args...).Scan(&expr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, datastore.NewCaveatNameNotFoundErr(name)
		}
		return nil, err
	}
	return &core.Caveat{
		Name:       name,
		Expression: expr,
	}, nil
}

func (rwt *pgReadWriteTXN) WriteCaveats(caveats []*core.Caveat) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(rwt.ctx), "WriteCaveats")
	defer span.End()

	deletedCaveatClause := sq.Or{}
	write := writeCaveat
	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		write = writeCaveatDeprecated
	}
	writtenCaveatNames := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		deletedCaveatClause = append(deletedCaveatClause, sq.Eq{colCaveatName: caveat.Name})
		valuesToWrite := []any{caveat.Name, caveat.Expression}
		// TODO remove once the ID->XID migrations are all complete
		if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
			valuesToWrite = append(valuesToWrite, rwt.newXID.Uint)
		}
		write = write.Values(valuesToWrite...)
		writtenCaveatNames = append(writtenCaveatNames, caveat.Name)
	}
	span.SetAttributes(common.CaveatNameKey.StringSlice(writtenCaveatNames))

	// mark current caveats as deleted
	err := rwt.deleteCaveatsWithClause(ctx, deletedCaveatClause)
	if err != nil {
		return err
	}

	// store the new caveat revision
	sql, args, err := write.ToSql()
	if err != nil {
		return fmt.Errorf("unable to write new caveat revision: %w", err)
	}
	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("unable to write new caveat revision: %w", err)
	}
	return nil
}

func (rwt *pgReadWriteTXN) DeleteCaveats(caveats []*core.Caveat) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(rwt.ctx), "DeleteCaveats")
	defer span.End()

	deletedCaveatClause := sq.Or{}
	deletedCaveatNames := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		deletedCaveatClause = append(deletedCaveatClause, sq.Eq{colCaveatName: caveat.Name})
		deletedCaveatNames = append(deletedCaveatNames, caveat.Name)
	}
	span.SetAttributes(common.CaveatNameKey.StringSlice(deletedCaveatNames))

	// mark current caveats as deleted
	return rwt.deleteCaveatsWithClause(ctx, deletedCaveatClause)
}

func (rwt *pgReadWriteTXN) deleteCaveatsWithClause(ctx context.Context, deleteClauses sq.Or) error {
	sql, args, err := deleteCaveat.
		Set(colDeletedXid, rwt.newXID).
		Where(sq.And{sq.Eq{colDeletedXid: liveDeletedTxnID}, deleteClauses}).
		ToSql()
	if err != nil {
		return fmt.Errorf("unable to mark previous caveat revisions as deleted: %w", err)
	}

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		baseQuery := deleteCaveat
		if rwt.migrationPhase == writeBothReadOld {
			baseQuery = deleteCaveatDeprecated
		}

		sql, args, err = baseQuery.
			Where(deleteClauses).
			Set(colDeletedTxnDeprecated, rwt.newXID.Uint).
			Set(colDeletedXid, rwt.newXID).
			ToSql()
		if err != nil {
			return fmt.Errorf("unable to mark previous caveat revisions as deleted: %w", err)
		}
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("unable to mark previous caveat revisions as deleted: %w", err)
	}
	return nil
}
