package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type pgReader struct {
	txSource       pgxcommon.TxFactory
	querySplitter  common.TupleQuerySplitter
	filterer       queryFilterer
	migrationPhase migrationPhase
}

type queryFilterer func(original sq.SelectBuilder) sq.SelectBuilder

var (
	queryTuples = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
	).From(tableTuple)

	schema = common.SchemaInformation{
		ColNamespace:        colNamespace,
		ColObjectID:         colObjectID,
		ColRelation:         colRelation,
		ColUsersetNamespace: colUsersetNamespace,
		ColUsersetObjectID:  colUsersetObjectID,
		ColUsersetRelation:  colUsersetRelation,
		ColCaveatName:       colCaveatContextName,
	}

	readNamespace = psql.Select(colConfig, colCreatedXid).From(tableNamespace)

	readNamespaceDeprecated = psql.Select(colConfig, colCreatedTxnDeprecated).From(tableNamespace)
)

const (
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
)

func (r *pgReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, r.filterer(queryTuples)).FilterWithRelationshipsFilter(filter)
	return r.querySplitter.SplitAndExecuteQuery(ctx, qBuilder, opts...)
}

func (r *pgReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.RelationshipIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, r.filterer(queryTuples)).
		FilterWithSubjectsFilter(subjectsFilter)

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	if queryOpts.ResRelation != nil {
		qBuilder = qBuilder.
			FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	return r.querySplitter.SplitAndExecuteQuery(ctx,
		qBuilder,
		options.WithLimit(queryOpts.ReverseLimit),
	)
}

func (r *pgReader) ReadNamespace(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	tx, txCleanup, err := r.txSource(ctx)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
	defer txCleanup(ctx)

	loaded, version, err := r.loadNamespace(ctx, nsName, tx, r.filterer)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return nil, datastore.NoRevision, err
	case err == nil:
		return loaded, version, nil
	default:
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}
}

func (r *pgReader) loadNamespace(ctx context.Context, namespace string, tx pgx.Tx, filterer queryFilterer) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "loadNamespace")
	defer span.End()

	defs, err := loadAllNamespaces(ctx, tx, func(original sq.SelectBuilder) sq.SelectBuilder {
		return filterer(original).Where(sq.Eq{colNamespace: namespace})
	}, r.migrationPhase)
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	if len(defs) < 1 {
		return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(namespace)
	}

	return defs[0].nsDef, defs[0].revision, nil
}

func (r *pgReader) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, txCleanup, err := r.txSource(ctx)
	if err != nil {
		return nil, err
	}
	defer txCleanup(ctx)

	nsDefsWithRevisions, err := loadAllNamespaces(ctx, tx, r.filterer, r.migrationPhase)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return stripRevisions(nsDefsWithRevisions), err
}

func (r *pgReader) LookupNamespaces(ctx context.Context, nsNames []string) ([]*core.NamespaceDefinition, error) {
	if len(nsNames) == 0 {
		return nil, nil
	}

	ctx = datastore.SeparateContextWithTracing(ctx)

	tx, txCleanup, err := r.txSource(ctx)
	if err != nil {
		return nil, err
	}
	defer txCleanup(ctx)

	clause := sq.Or{}
	for _, nsName := range nsNames {
		clause = append(clause, sq.Eq{colNamespace: nsName})
	}

	nsDefsWithRevisions, err := loadAllNamespaces(ctx, tx, func(original sq.SelectBuilder) sq.SelectBuilder {
		return r.filterer(original).Where(clause)
	}, r.migrationPhase)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return stripRevisions(nsDefsWithRevisions), err
}

func stripRevisions(defsWithRevisions []nsAndVersion) []*core.NamespaceDefinition {
	nsDefs := make([]*core.NamespaceDefinition, 0, len(defsWithRevisions))
	for _, defWithRevision := range defsWithRevisions {
		nsDefs = append(nsDefs, defWithRevision.nsDef)
	}
	return nsDefs
}

type nsAndVersion struct {
	nsDef    *core.NamespaceDefinition
	revision datastore.Revision
}

func loadAllNamespaces(
	ctx context.Context,
	tx pgx.Tx,
	filterer queryFilterer,
	migrationPhase migrationPhase,
) ([]nsAndVersion, error) {
	baseQuery := readNamespace

	// TODO remove once the ID->XID migrations are all complete
	if migrationPhase == writeBothReadOld {
		baseQuery = readNamespaceDeprecated
	}

	sql, args, err := filterer(baseQuery).ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nsDefs []nsAndVersion
	for rows.Next() {
		var config []byte
		var version xid8

		var versionDest interface{} = &version

		// TODO remove once the ID->XID migrations are all complete
		var versionTxDeprecated uint64
		if migrationPhase == writeBothReadOld {
			versionDest = &versionTxDeprecated
		}

		if err := rows.Scan(&config, versionDest); err != nil {
			return nil, err
		}

		loaded := &core.NamespaceDefinition{}
		if err := loaded.UnmarshalVT(config); err != nil {
			return nil, fmt.Errorf(errUnableToReadConfig, err)
		}

		revision := revisionFromTransaction(version)

		// TODO remove once the ID->XID migrations are all complete
		if migrationPhase == writeBothReadOld {
			revision = decimal.NewFromInt(int64(versionTxDeprecated))
		}

		nsDefs = append(nsDefs, nsAndVersion{loaded, revision})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return nsDefs, nil
}

var _ datastore.Reader = &pgReader{}
