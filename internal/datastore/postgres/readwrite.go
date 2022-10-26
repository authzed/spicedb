package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToWriteConfig         = "unable to write namespace config: %w"
	errUnableToDeleteConfig        = "unable to delete namespace config: %w"
	errUnableToWriteRelationships  = "unable to write relationships: %w"
	errUnableToDeleteRelationships = "unable to delete relationships: %w"
)

var (
	writeNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
	)
	// TODO remove once the ID->XID migrations are all complete
	writeNamespaceDeprecated = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
		colCreatedTxnDeprecated,
	)

	deleteNamespace = psql.Update(tableNamespace).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})

	deleteNamespaceTuples = psql.Update(tableTuple).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})

	writeTuple = psql.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
	)

	// TODO remove once the ID->XID migrations are all complete
	writeTupleDeprecated = psql.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
		colCreatedTxnDeprecated,
	)

	deleteTuple = psql.Update(tableTuple).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})

	// TODO remove once the ID->XID migrations are all complete
	deleteTupleDeprecated = psql.Update(tableTuple).Where(sq.Eq{colDeletedTxnDeprecated: liveDeletedTxnID})
)

type pgReadWriteTXN struct {
	*pgReader
	tx             pgx.Tx
	newXID         xid8
	migrationPhase migrationPhase
}

func (rwt *pgReadWriteTXN) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()

	bulkWrite := writeTuple

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		bulkWrite = writeTupleDeprecated
	}

	bulkWriteHasValues := false

	deleteClauses := sq.Or{}

	// Process the actual updates
	for _, mut := range mutations {
		tpl := mut.Tuple

		if mut.Operation == core.RelationTupleUpdate_TOUCH || mut.Operation == core.RelationTupleUpdate_DELETE {
			deleteClauses = append(deleteClauses, exactRelationshipClause(tpl))
		}

		if mut.Operation == core.RelationTupleUpdate_TOUCH || mut.Operation == core.RelationTupleUpdate_CREATE {
			var caveatName string
			var caveatContext map[string]any
			if tpl.Caveat != nil {
				caveatName = tpl.Caveat.CaveatName
				caveatContext = tpl.Caveat.Context.AsMap()
			}
			valuesToWrite := []interface{}{
				tpl.ResourceAndRelation.Namespace,
				tpl.ResourceAndRelation.ObjectId,
				tpl.ResourceAndRelation.Relation,
				tpl.Subject.Namespace,
				tpl.Subject.ObjectId,
				tpl.Subject.Relation,
				caveatName,
				caveatContext, // PGX driver serializes map[string]any to JSONB type columns
			}

			// TODO remove once the ID->XID migrations are all complete
			if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
				valuesToWrite = append(valuesToWrite, rwt.newXID.Uint)
			}

			bulkWrite = bulkWrite.Values(valuesToWrite...)
			bulkWriteHasValues = true
		}
	}

	if len(deleteClauses) > 0 {
		sql, args, err := deleteTuple.
			Where(deleteClauses).
			Set(colDeletedXid, rwt.newXID).
			ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		// TODO remove once the ID->XID migrations are all complete
		if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
			baseQuery := deleteTuple
			if rwt.migrationPhase == writeBothReadOld {
				baseQuery = deleteTupleDeprecated
			}

			sql, args, err = baseQuery.
				Where(deleteClauses).
				Set(colDeletedTxnDeprecated, rwt.newXID.Uint).
				Set(colDeletedXid, rwt.newXID).
				ToSql()
			if err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
		}

		if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	if bulkWriteHasValues {
		sql, args, err := bulkWrite.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
			// If a unique constraint violation is returned, then its likely that the cause
			// was an existing relationship given as a CREATE.
			if cerr := pgxcommon.ConvertToWriteConstraintError(livingTupleConstraint, err); cerr != nil {
				return cerr
			}

			// TODO remove once the ID->XID migrations are all complete
			if cerr := pgxcommon.ConvertToWriteConstraintError(livingTupleConstraintOld, err); cerr != nil {
				return cerr
			}

			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func (rwt *pgReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()

	// Add clauses for the ResourceFilter
	query := deleteTuple.Where(sq.Eq{colNamespace: filter.ResourceType})
	tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.ResourceType)}
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
		tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(filter.OptionalResourceId))
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
		tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(filter.OptionalRelation))
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		tracerAttributes = append(tracerAttributes, common.SubNamespaceNameKey.String(subjectFilter.SubjectType))
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
			tracerAttributes = append(tracerAttributes, common.SubObjectIDKey.String(subjectFilter.OptionalSubjectId))
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
			tracerAttributes = append(tracerAttributes, common.SubRelationNameKey.String(relationFilter.Relation))
		}
	}

	span.SetAttributes(tracerAttributes...)

	sql, args, err := query.Set(colDeletedXid, rwt.newXID).ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		sql, args, err = query.Set(colDeletedTxnDeprecated, rwt.newXID.Uint).Set(colDeletedXid, rwt.newXID).ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToDeleteRelationships, err)
		}
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteNamespaces")
	defer span.End()

	deletedNamespaceClause := sq.Or{}
	writeQuery := writeNamespace

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		writeQuery = writeNamespaceDeprecated
	}

	writtenNamespaceNames := make([]string, 0, len(newConfigs))
	for _, newNamespace := range newConfigs {
		serialized, err := proto.Marshal(newNamespace)
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}
		span.AddEvent("Serialized namespace config")

		deletedNamespaceClause = append(deletedNamespaceClause, sq.Eq{colNamespace: newNamespace.Name})

		valuesToWrite := []interface{}{newNamespace.Name, serialized}

		// TODO remove once the ID->XID migrations are all complete
		if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
			valuesToWrite = append(valuesToWrite, rwt.newXID.Uint)
		}

		writeQuery = writeQuery.Values(valuesToWrite...)
		writtenNamespaceNames = append(writtenNamespaceNames, newNamespace.Name)
	}

	span.SetAttributes(common.ObjNamespaceNameKey.StringSlice(writtenNamespaceNames))

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedXid, rwt.newXID).
		Where(sq.And{sq.Eq{colDeletedXid: liveDeletedTxnID}, deletedNamespaceClause}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		whereClause := sq.Eq{colDeletedXid: liveDeletedTxnID}
		if rwt.migrationPhase == writeBothReadOld {
			whereClause = sq.Eq{colDeletedTxnDeprecated: liveDeletedTxnID}
		}

		delSQL, delArgs, err = deleteNamespace.
			Set(colDeletedTxnDeprecated, rwt.newXID.Uint).
			Set(colDeletedXid, rwt.newXID).
			Where(sq.And{whereClause, deletedNamespaceClause}).
			ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	sql, args, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config written")

	return nil
}

func (rwt *pgReadWriteTXN) DeleteNamespace(ctx context.Context, nsName string) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteNamespace")
	defer span.End()

	filterer := func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
	}

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadOld {
		filterer = func(original sq.SelectBuilder) sq.SelectBuilder {
			return original.Where(sq.Eq{colDeletedTxnDeprecated: liveDeletedTxnID})
		}
	}

	_, createdAt, err := rwt.loadNamespace(ctx, nsName, rwt.tx, filterer)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return err
	case err == nil:
		break
	default:
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedXid, rwt.newXID).
		Where(sq.Eq{colNamespace: nsName, colCreatedXid: createdAt.tx}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		whereClause := sq.Eq{colNamespace: nsName, colCreatedXid: createdAt.tx}
		if rwt.migrationPhase == writeBothReadOld {
			whereClause = sq.Eq{colNamespace: nsName, colCreatedTxnDeprecated: createdAt.tx.Uint}
		}

		delSQL, delArgs, err = deleteNamespace.
			Set(colDeletedTxnDeprecated, rwt.newXID.Uint).
			Set(colDeletedXid, rwt.newXID).
			Where(whereClause).
			ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := deleteNamespaceTuples.
		Set(colDeletedXid, rwt.newXID).
		Where(sq.Eq{colNamespace: nsName}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// TODO remove once the ID->XID migrations are all complete
	if rwt.migrationPhase == writeBothReadNew || rwt.migrationPhase == writeBothReadOld {
		deleteTupleSQL, deleteTupleArgs, err = deleteNamespaceTuples.
			Set(colDeletedTxnDeprecated, rwt.newXID.Uint).
			Set(colDeletedXid, rwt.newXID).
			Where(sq.Eq{colNamespace: nsName}).
			ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}
	}

	_, err = rwt.tx.Exec(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return nil
}

func exactRelationshipClause(r *core.RelationTuple) sq.Eq {
	return sq.Eq{
		colNamespace:        r.ResourceAndRelation.Namespace,
		colObjectID:         r.ResourceAndRelation.ObjectId,
		colRelation:         r.ResourceAndRelation.Relation,
		colUsersetNamespace: r.Subject.Namespace,
		colUsersetObjectID:  r.Subject.ObjectId,
		colUsersetRelation:  r.Subject.Relation,
	}
}

var _ datastore.ReadWriteTransaction = &pgReadWriteTXN{}
