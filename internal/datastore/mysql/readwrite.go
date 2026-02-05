package mysql

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast/v2"
	"github.com/go-sql-driver/mysql"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	schemaadapter "github.com/authzed/spicedb/internal/datastore/schema"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	errUnableToWriteRelationships     = "unable to write relationships: %w"
	errUnableToBulkWriteRelationships = "unable to bulk write relationships: %w"
	errUnableToDeleteRelationships    = "unable to delete relationships: %w"
	errUnableToWriteConfig            = "unable to write namespace config: %w"
	errUnableToDeleteConfig           = "unable to delete namespace config: %w"

	bulkInsertRowsLimit = 1_000
)

var (
	duplicateEntryRegex          = regexp.MustCompile(`^Duplicate entry '(.+)' for key 'uq_relation_tuple_living'$`)
	duplicateEntryFullIndexRegex = regexp.MustCompile(`^Duplicate entry '(.+)' for key 'relation_tuple.uq_relation_tuple_living'$`)
)

type mysqlReadWriteTXN struct {
	*mysqlReader

	tupleTableName          string
	schemaTableName         string
	schemaRevisionTableName string
	tx                      *sql.Tx
	newTxnID                uint64
}

// structpbWrapper is used to marshall maps into MySQLs JSON data type
type structpbWrapper map[string]any

func (cc *structpbWrapper) Scan(val any) error {
	if val == nil {
		clear(*cc)
		*cc = nil
		return nil
	}

	v, ok := val.([]byte)
	if !ok {
		return fmt.Errorf("unsupported type: %T", v)
	}

	clear(*cc)
	return json.Unmarshal(v, cc)
}

func (cc *structpbWrapper) Value() (driver.Value, error) {
	return json.Marshal(&cc)
}

func (rwt *mysqlReadWriteTXN) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	// Check if the counter already exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) > 0 {
		return datastore.NewCounterAlreadyRegisteredErr(name, filter)
	}

	serializedFilter, err := filter.MarshalVT()
	if err != nil {
		return fmt.Errorf("unable to serialize filter: %w", err)
	}

	// Insert the counter.
	query, args, err := rwt.InsertCounterQuery.
		Values(
			name,
			serializedFilter,
			0,
			0,
			rwt.newTxnID,
		).ToSql()
	if err != nil {
		return fmt.Errorf("unable to register counter: %w", err)
	}

	_, err = rwt.tx.ExecContext(ctx, query, args...)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == errMysqlDuplicateEntry {
			return datastore.NewCounterAlreadyRegisteredErr(name, filter)
		}

		return fmt.Errorf("unable to register counter: %w", err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) UnregisterCounter(ctx context.Context, name string) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	// Delete the counter.
	query, args, err := rwt.DeleteCounterQuery.
		Where(sq.Eq{colName: name}).
		Set(colDeletedTxn, rwt.newTxnID).
		ToSql()
	if err != nil {
		return fmt.Errorf("unable to unregister counter: %w", err)
	}

	_, err = rwt.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("unable to unregister counter: %w", err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	updateRevisionID := computedAtRevision.(revisions.TransactionIDRevision).TransactionID()

	// Update the counter.
	query, args, err := rwt.UpdateCounterQuery.
		Where(sq.Eq{colName: name}).
		Set(colCounterCurrentCount, value).
		Set(colCounterUpdatedAtRevision, updateRevisionID).
		ToSql()
	if err != nil {
		return fmt.Errorf("unable to store counter value: %w", err)
	}

	_, err = rwt.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("unable to store counter value: %w", err)
	}

	return nil
}

// WriteRelationships takes a list of existing relationships that must exist, and a list of
// tuple mutations and applies it to the datastore for the specified namespace.
func (rwt *mysqlReadWriteTXN) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	// TODO(jschorr): Determine if we can do this in a more efficient manner using ON CONFLICT UPDATE
	// rather than SELECT FOR UPDATE as we've been doing.
	bulkWrite := rwt.WriteRelsQuery
	bulkWriteHasValues := false

	selectForUpdateQuery := rwt.QueryRelsWithIdsQuery

	clauses := sq.Or{}
	createAndTouchMutationsByRel := make(map[string]tuple.RelationshipUpdate, len(mutations))

	// Collect all TOUCH and DELETE operations. CREATE is handled below.
	for _, mut := range mutations {
		rel := mut.Relationship
		relString := tuple.StringWithoutCaveatOrExpiration(rel)

		switch mut.Operation {
		case tuple.UpdateOperationCreate:
			createAndTouchMutationsByRel[relString] = mut

		case tuple.UpdateOperationTouch:
			createAndTouchMutationsByRel[relString] = mut
			clauses = append(clauses, exactRelationshipClause(rel))

		case tuple.UpdateOperationDelete:
			clauses = append(clauses, exactRelationshipClause(rel))

		default:
			return spiceerrors.MustBugf("unknown mutation operation")
		}
	}

	if len(clauses) > 0 {
		query, args, err := selectForUpdateQuery.
			Where(clauses).
			Where(sq.GtOrEq{colDeletedTxn: rwt.newTxnID}).
			ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		rows, err := rwt.tx.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
		defer common.LogOnError(ctx, rows.Close)

		var resourceObjectType string
		var resourceObjectID string
		var relation string
		var subjectObjectType string
		var subjectObjectID string
		var subjectRelation string
		var caveatName string
		var caveatContext structpbWrapper
		var expiration *time.Time

		relIdsToDelete := make([]int64, 0, len(clauses))
		for rows.Next() {
			var relationshipID int64
			if err := rows.Scan(
				&relationshipID,
				&resourceObjectType,
				&resourceObjectID,
				&relation,
				&subjectObjectType,
				&subjectObjectID,
				&subjectRelation,
				&caveatName,
				&caveatContext,
				&expiration,
			); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}

			foundRel := tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: resourceObjectType,
						ObjectID:   resourceObjectID,
						Relation:   relation,
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: subjectObjectType,
						ObjectID:   subjectObjectID,
						Relation:   subjectRelation,
					},
				},
				OptionalExpiration: expiration,
			}

			// if the relationship to be deleted is for a TOUCH operation and the caveat
			// name or context has not changed, then remove it from delete and create.
			tplString := tuple.StringWithoutCaveatOrExpiration(foundRel)
			if mut, ok := createAndTouchMutationsByRel[tplString]; ok {
				foundRel.OptionalCaveat, err = common.ContextualizedCaveatFrom(caveatName, caveatContext)
				if err != nil {
					return fmt.Errorf(errUnableToQueryTuples, err)
				}

				// Ensure the tuples are the same.
				if tuple.Equal(mut.Relationship, foundRel) {
					delete(createAndTouchMutationsByRel, tplString)
					continue
				}
			}

			relIdsToDelete = append(relIdsToDelete, relationshipID)
		}

		if rows.Err() != nil {
			return fmt.Errorf(errUnableToWriteRelationships, rows.Err())
		}

		if len(relIdsToDelete) > 0 {
			query, args, err := rwt.
				DeleteRelsQuery.
				Where(sq.Eq{colID: relIdsToDelete}).
				Set(colDeletedTxn, rwt.newTxnID).
				ToSql()
			if err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
			if _, err := rwt.tx.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
		}
	}

	for _, mut := range createAndTouchMutationsByRel {
		rel := mut.Relationship

		var caveatName string
		var caveatContext structpbWrapper
		if rel.OptionalCaveat != nil {
			caveatName = rel.OptionalCaveat.CaveatName
			caveatContext = rel.OptionalCaveat.Context.AsMap()
		}
		bulkWrite = bulkWrite.Values(
			rel.Resource.ObjectType,
			rel.Resource.ObjectID,
			rel.Resource.Relation,
			rel.Subject.ObjectType,
			rel.Subject.ObjectID,
			rel.Subject.Relation,
			caveatName,
			&caveatContext,
			rel.OptionalExpiration,
			rwt.newTxnID,
		)
		bulkWriteHasValues = true
	}

	if bulkWriteHasValues {
		query, args, err := bulkWrite.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		_, err = rwt.tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (uint64, bool, error) {
	// Add clauses for the ResourceFilter
	query := rwt.DeleteRelsQuery
	if filter.ResourceType != "" {
		query = query.Where(sq.Eq{colNamespace: filter.ResourceType})
	}
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
	}
	if filter.OptionalResourceIdPrefix != "" {
		likeClause, err := common.BuildLikePrefixClause(colObjectID, filter.OptionalResourceIdPrefix)
		if err != nil {
			return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
		}
		query = query.Where(likeClause)
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{colUsersetRelation: cmp.Or(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	query = query.Set(colDeletedTxn, rwt.newTxnID)

	// Add the limit, if any.
	delOpts := options.NewDeleteOptionsWithOptionsAndDefaults(opts...)
	var delLimit uint64
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		delLimit = *delOpts.DeleteLimit
	}

	if delLimit > 0 {
		query = query.Limit(delLimit)
	}

	querySQL, args, err := query.ToSql()
	if err != nil {
		return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	modified, err := rwt.tx.ExecContext(ctx, querySQL, args...)
	if err != nil {
		return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	rowsAffected, err := modified.RowsAffected()
	if err != nil {
		return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	uintRowsAffected, err := safecast.Convert[uint64](rowsAffected)
	if err != nil {
		return 0, false, spiceerrors.MustBugf("rowsAffected was negative: %v", err)
	}

	if delLimit > 0 && uintRowsAffected == delLimit {
		return uintRowsAffected, true, nil
	}

	return uintRowsAffected, false, nil
}

func (rwt *mysqlReadWriteTXN) LegacyWriteNamespaces(ctx context.Context, newNamespaces ...*core.NamespaceDefinition) error {
	deletedNamespaceClause := sq.Or{}
	writeQuery := rwt.WriteNamespaceQuery

	for _, newNamespace := range newNamespaces {
		serialized, err := newNamespace.MarshalVT()
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}

		deletedNamespaceClause = append(deletedNamespaceClause, sq.Eq{colNamespace: newNamespace.Name})
		writeQuery = writeQuery.Values(newNamespace.Name, serialized, rwt.newTxnID)
	}

	delSQL, delArgs, err := rwt.DeleteNamespaceQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.And{sq.Eq{colDeletedTxn: liveDeletedTxnID}, deletedNamespaceClause}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	query, args, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	// Write the schema hash to the schema_revision table for fast lookups
	if err := rwt.writeLegacySchemaHash(ctx); err != nil {
		return fmt.Errorf("failed to write schema hash: %w", err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	if len(nsNames) == 0 {
		return nil
	}

	// For each namespace, check they exist and collect predicates for the
	// "WHERE" clause to delete the namespaces and (if requested) associated tuples.
	nsClauses := make([]sq.Sqlizer, 0, len(nsNames))
	tplClauses := make([]sq.Sqlizer, 0, len(nsNames))
	for _, nsName := range nsNames {
		// TODO(jzelinskie): check these in one query
		baseQuery := rwt.ReadNamespaceQuery.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
		_, createdAt, err := loadNamespace(ctx, nsName, rwt.tx, baseQuery)
		switch {
		case errors.As(err, &datastore.NamespaceNotFoundError{}):
			// TODO(jzelinskie): return the name of the missing namespace
			return err
		case err == nil:
			nsClauses = append(nsClauses, sq.Eq{colNamespace: nsName, colCreatedTxn: createdAt})
			tplClauses = append(tplClauses, sq.Eq{colNamespace: nsName})
		default:
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}
	}

	delSQL, delArgs, err := rwt.DeleteNamespaceQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Or(nsClauses)).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	if delOption == datastore.DeleteNamespacesAndRelationships {
		deleteTupleSQL, deleteTupleArgs, err := rwt.DeleteNamespaceRelationshipsQuery.
			Set(colDeletedTxn, rwt.newTxnID).
			Where(sq.Or(tplClauses)).
			ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}

		_, err = rwt.tx.ExecContext(ctx, deleteTupleSQL, deleteTupleArgs...)
		if err != nil {
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}
	}

	// Write the schema hash to the schema_revision table for fast lookups
	if err := rwt.writeLegacySchemaHash(ctx); err != nil {
		return fmt.Errorf("failed to write schema hash: %w", err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) SchemaWriter() (datastore.SchemaWriter, error) {
	// Wrap the transaction with an unexported schema writer
	writer := &mysqlSchemaWriter{rwt: rwt}
	return schemaadapter.NewSchemaWriter(writer, writer, rwt.schemaMode), nil
}

// mysqlSchemaWriter wraps a mysqlReadWriteTXN and implements DualSchemaWriter.
// This prevents direct access to schema write methods from the transaction.
type mysqlSchemaWriter struct {
	rwt *mysqlReadWriteTXN
}

// WriteStoredSchema implements datastore.SingleStoreSchemaWriter by writing within the current transaction
func (w *mysqlSchemaWriter) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	// Create a transaction-aware executor that uses the current transaction
	executor := newMySQLTransactionAwareExecutor(w.rwt.tx)

	// Use the shared schema reader/writer to write the schema with the newTxnID as transaction ID
	if err := w.rwt.schemaReaderWriter.WriteSchema(ctx, schema, executor, func(ctx context.Context) uint64 {
		return w.rwt.newTxnID
	}); err != nil {
		return err
	}

	// Write the schema hash to the schema_revision table for fast lookups
	if err := w.writeSchemaHash(ctx, schema); err != nil {
		return fmt.Errorf("failed to write schema hash: %w", err)
	}

	return nil
}

// writeSchemaHash writes the schema hash to the schema_revision table
func (w *mysqlSchemaWriter) writeSchemaHash(ctx context.Context, schema *core.StoredSchema) error {
	v1 := schema.GetV1()
	if v1 == nil {
		return fmt.Errorf("unsupported schema version: %d", schema.Version)
	}

	// Mark existing hash rows as deleted
	sql, args, err := sb.Update(w.rwt.schemaRevisionTableName).
		Set("deleted_transaction", w.rwt.newTxnID).
		Where(sq.Eq{
			"name":                "current",
			"deleted_transaction": liveDeletedTxnID,
		}).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to build delete query: %w", err)
	}

	if _, err := w.rwt.tx.ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to delete old hash: %w", err)
	}

	// Insert new hash row (INSERT IGNORE handles WriteBoth mode duplicates)
	sql, args, err = sb.Insert(w.rwt.schemaRevisionTableName).
		Options("IGNORE").
		Columns("name", "hash", "created_transaction", "deleted_transaction").
		Values("current", []byte(v1.SchemaHash), w.rwt.newTxnID, liveDeletedTxnID).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to build insert query: %w", err)
	}

	if _, err := w.rwt.tx.ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to insert hash: %w", err)
	}

	return nil
}

// writeLegacySchemaHash writes the schema hash to the schema_revision table by generating
// the schema from current legacy namespaces and caveats
func (rwt *mysqlReadWriteTXN) writeLegacySchemaHash(ctx context.Context) error {
	// Read all namespaces and caveats
	namespaces, err := rwt.LegacyListAllNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	caveats, err := rwt.LegacyListAllCaveats(ctx)
	if err != nil {
		return fmt.Errorf("failed to list caveats: %w", err)
	}

	// Build schema definitions list
	var definitions []compiler.SchemaDefinition
	for _, ns := range namespaces {
		definitions = append(definitions, ns.Definition)
	}
	for _, caveat := range caveats {
		definitions = append(definitions, caveat.Definition)
	}

	// Sort definitions by name for consistent ordering
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].GetName() < definitions[j].GetName()
	})

	// Generate schema text from definitions
	schemaText, _, err := generator.GenerateSchema(definitions)
	if err != nil {
		// Log warning but don't fail - this can happen with intentionally corrupted data in tests
		// or if somehow invalid data gets written. The hash is an optimization for change detection.
		log.Ctx(ctx).Warn().Err(err).Msg("failed to generate schema for hash, skipping hash write")
		return nil
	}

	// Compute schema hash
	schemaHash := hex.EncodeToString([]byte(schemaText))

	// Mark existing hash rows as deleted
	sql, args, err := sb.Update(rwt.schemaRevisionTableName).
		Set("deleted_transaction", rwt.newTxnID).
		Where(sq.Eq{
			"name":                "current",
			"deleted_transaction": liveDeletedTxnID,
		}).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to build delete query: %w", err)
	}

	if _, err := rwt.tx.ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to delete old hash: %w", err)
	}

	// Insert new hash row (INSERT IGNORE handles WriteBoth mode duplicates)
	sql, args, err = sb.Insert(rwt.schemaRevisionTableName).
		Options("IGNORE").
		Columns("name", "hash", "created_transaction", "deleted_transaction").
		Values("current", []byte(schemaHash), rwt.newTxnID, liveDeletedTxnID).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to build insert query: %w", err)
	}

	if _, err := rwt.tx.ExecContext(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to insert hash: %w", err)
	}

	return nil
}

// ReadStoredSchema implements datastore.SingleStoreSchemaReader to satisfy DualSchemaReader interface requirements
func (w *mysqlSchemaWriter) ReadStoredSchema(ctx context.Context) (*core.StoredSchema, error) {
	// Create a revision-aware executor that applies alive filter
	executor := &mysqlRevisionAwareExecutor{
		txSource:    w.rwt.txSource,
		aliveFilter: w.rwt.aliveFilter,
	}

	// Use the shared schema reader/writer to read the schema
	// Pass nil for transaction reads to bypass cache
	return w.rwt.schemaReaderWriter.ReadSchema(ctx, executor, nil)
}

// LegacyWriteNamespaces delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	return w.rwt.LegacyWriteNamespaces(ctx, newConfigs...)
}

// LegacyDeleteNamespaces delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	return w.rwt.LegacyDeleteNamespaces(ctx, nsNames, delOption)
}

// LegacyWriteCaveats delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	return w.rwt.LegacyWriteCaveats(ctx, caveats)
}

// LegacyDeleteCaveats delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	return w.rwt.LegacyDeleteCaveats(ctx, names)
}

// LegacyReadCaveatByName delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	return w.rwt.LegacyReadCaveatByName(ctx, name)
}

// LegacyListAllCaveats delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return w.rwt.LegacyListAllCaveats(ctx)
}

// LegacyLookupCaveatsWithNames delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyLookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	return w.rwt.LegacyLookupCaveatsWithNames(ctx, names)
}

// LegacyReadNamespaceByName delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	return w.rwt.LegacyReadNamespaceByName(ctx, nsName)
}

// LegacyListAllNamespaces delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	return w.rwt.LegacyListAllNamespaces(ctx)
}

// LegacyLookupNamespacesWithNames delegates to the underlying transaction
func (w *mysqlSchemaWriter) LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedDefinition[*core.NamespaceDefinition], error) {
	return w.rwt.LegacyLookupNamespacesWithNames(ctx, nsNames)
}

func (rwt *mysqlReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	var sqlStmt bytes.Buffer

	sql, _, err := rwt.WriteRelsQuery.Values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).ToSql()
	if err != nil {
		return 0, err
	}

	var numWritten uint64
	var rel *tuple.Relationship

	// Bootstrap the loop
	rel, err = iter.Next(ctx)

	for rel != nil && err == nil {
		sqlStmt.Reset()
		sqlStmt.WriteString(sql)
		var args []any
		var batchLen uint64

		for ; rel != nil && err == nil && batchLen < bulkInsertRowsLimit; rel, err = iter.Next(ctx) {
			if batchLen != 0 {
				sqlStmt.WriteString(",(?,?,?,?,?,?,?,?,?,?)")
			}

			var caveatName string
			var caveatContext structpbWrapper
			if rel.OptionalCaveat != nil {
				caveatName = rel.OptionalCaveat.CaveatName
				caveatContext = rel.OptionalCaveat.Context.AsMap()
			}
			args = append(args,
				rel.Resource.ObjectType,
				rel.Resource.ObjectID,
				rel.Resource.Relation,
				rel.Subject.ObjectType,
				rel.Subject.ObjectID,
				rel.Subject.Relation,
				caveatName,
				&caveatContext,
				rel.OptionalExpiration,
				rwt.newTxnID,
			)
			batchLen++
		}
		if err != nil {
			return 0, fmt.Errorf(errUnableToBulkWriteRelationships, err)
		}

		if batchLen > 0 {
			log.Warn().Uint64("count", batchLen).Uint64("written", numWritten).Msg("writing batch")
			if _, err := rwt.tx.Exec(sqlStmt.String(), args...); err != nil {
				return 0, fmt.Errorf(errUnableToBulkWriteRelationships, fmt.Errorf("error writing batch: %w", err))
			}
		}

		numWritten += batchLen
	}
	if err != nil {
		return 0, fmt.Errorf(errUnableToBulkWriteRelationships, err)
	}

	return numWritten, nil
}

func convertToWriteConstraintError(err error) error {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == errMysqlDuplicateEntry {
		found := duplicateEntryRegex.FindStringSubmatch(mysqlErr.Message)
		if found != nil {
			parts := strings.Split(found[1], "-")
			if len(parts) == 7 {
				return common.NewCreateRelationshipExistsError(&tuple.Relationship{
					RelationshipReference: tuple.RelationshipReference{
						Resource: tuple.ObjectAndRelation{
							ObjectType: parts[0],
							ObjectID:   parts[1],
							Relation:   parts[2],
						},
						Subject: tuple.ObjectAndRelation{
							ObjectType: parts[3],
							ObjectID:   parts[4],
							Relation:   parts[5],
						},
					},
				})
			}
		}

		found = duplicateEntryFullIndexRegex.FindStringSubmatch(mysqlErr.Message)
		if found != nil {
			parts := strings.Split(found[1], "-")
			if len(parts) == 7 {
				return common.NewCreateRelationshipExistsError(&tuple.Relationship{
					RelationshipReference: tuple.RelationshipReference{
						Resource: tuple.ObjectAndRelation{
							ObjectType: parts[0],
							ObjectID:   parts[1],
							Relation:   parts[2],
						},
						Subject: tuple.ObjectAndRelation{
							ObjectType: parts[3],
							ObjectID:   parts[4],
							Relation:   parts[5],
						},
					},
				})
			}
		}

		return common.NewCreateRelationshipExistsError(nil)
	}
	return nil
}

func exactRelationshipClause(r tuple.Relationship) sq.Eq {
	return sq.Eq{
		colNamespace:        r.Resource.ObjectType,
		colObjectID:         r.Resource.ObjectID,
		colRelation:         r.Resource.Relation,
		colUsersetNamespace: r.Subject.ObjectType,
		colUsersetObjectID:  r.Subject.ObjectID,
		colUsersetRelation:  r.Subject.Relation,
	}
}

var (
	_ datastore.ReadWriteTransaction = &mysqlReadWriteTXN{}
	_ datastore.LegacySchemaWriter   = &mysqlReadWriteTXN{}
	_ datastore.DualSchemaWriter     = &mysqlSchemaWriter{}
	_ datastore.DualSchemaReader     = &mysqlSchemaWriter{}
)
