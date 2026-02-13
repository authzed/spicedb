package spanner

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	schemaadapter "github.com/authzed/spicedb/internal/datastore/schema"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type spannerReadWriteTXN struct {
	spannerReader
	spannerRWT *spanner.ReadWriteTransaction

	// IMPORTANT: Spanner Read-Write Transaction Limitation
	// =====================================================
	// In Cloud Spanner, reads within a read-write transaction do NOT see the effects of
	// buffered writes (mutations) performed earlier in that same transaction. This is because
	// writes are buffered locally at the client and are not sent to the server until commit.
	// This is a fundamental Spanner design, not an emulator limitation.
	//
	// To work around this, we track all schema writes and deletes in memory maps below.
	// When List methods are called, we merge buffered writes with committed data read from
	// Spanner, ensuring the legacy schema writer can correctly compute diffs without attempting
	// to read buffered writes from Spanner.

	// bufferedNamespaces tracks namespaces written in this transaction
	bufferedNamespaces map[string]*core.NamespaceDefinition

	// deletedNamespaces tracks namespaces deleted in this transaction
	deletedNamespaces map[string]struct{}

	// bufferedCaveats tracks caveats written in this transaction
	bufferedCaveats map[string]*core.CaveatDefinition

	// deletedCaveats tracks caveats deleted in this transaction
	deletedCaveats map[string]struct{}
}

const inLimit = 10_000 // https://cloud.google.com/spanner/quotas#query-limits

// LegacyListAllNamespaces reads namespaces from Spanner and merges them with any buffered writes.
// This is necessary because in Spanner, buffered writes in a read-write transaction are not visible
// to reads in the same transaction. The buffered map contains namespaces written in this transaction,
// and the deleted map tracks namespaces deleted in this transaction.
func (rwt *spannerReadWriteTXN) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	// First, read from Spanner (this will get committed data, not buffered writes)
	existing, err := rwt.spannerReader.LegacyListAllNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	// Build a map of existing namespaces by name, excluding deleted ones
	merged := make(map[string]datastore.RevisionedNamespace)
	for _, ns := range existing {
		if _, deleted := rwt.deletedNamespaces[ns.Definition.Name]; !deleted {
			merged[ns.Definition.Name] = ns
		}
	}

	// Overlay buffered writes (these override anything read from Spanner)
	for name, def := range rwt.bufferedNamespaces {
		merged[name] = datastore.RevisionedNamespace{
			Definition:          def,
			LastWrittenRevision: datastore.NoRevision, // Will be set on commit
		}
	}

	// Convert map back to slice
	result := make([]datastore.RevisionedNamespace, 0, len(merged))
	for _, ns := range merged {
		result = append(result, ns)
	}

	return result, nil
}

// LegacyListAllCaveats reads caveats from Spanner and merges them with any buffered writes.
// See LegacyListAllNamespaces for the rationale.
func (rwt *spannerReadWriteTXN) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	// First, read from Spanner (this will get committed data, not buffered writes)
	existing, err := rwt.spannerReader.LegacyListAllCaveats(ctx)
	if err != nil {
		return nil, err
	}

	// Build a map of existing caveats by name, excluding deleted ones
	merged := make(map[string]datastore.RevisionedCaveat)
	for _, caveat := range existing {
		if _, deleted := rwt.deletedCaveats[caveat.Definition.Name]; !deleted {
			merged[caveat.Definition.Name] = caveat
		}
	}

	// Overlay buffered writes (these override anything read from Spanner)
	for name, def := range rwt.bufferedCaveats {
		merged[name] = datastore.RevisionedCaveat{
			Definition:          def,
			LastWrittenRevision: datastore.NoRevision, // Will be set on commit
		}
	}

	// Convert map back to slice
	result := make([]datastore.RevisionedCaveat, 0, len(merged))
	for _, caveat := range merged {
		result = append(result, caveat)
	}

	return result, nil
}

func (rwt *spannerReadWriteTXN) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	// Ensure the counter doesn't already exist.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) > 0 {
		return datastore.NewCounterAlreadyRegisteredErr(name, filter)
	}

	// Add the counter to the table.
	serialized, err := filter.MarshalVT()
	if err != nil {
		return fmt.Errorf(errUnableToSerializeFilter, err)
	}

	if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
		spanner.InsertOrUpdate(
			tableRelationshipCounter,
			[]string{colCounterName, colCounterSerializedFilter, colCounterCurrentCount, colCounterUpdatedAtTimestamp},
			[]any{name, serialized, 0, nil},
		),
	}); err != nil {
		return fmt.Errorf(errUnableToWriteCounter, err)
	}

	return nil
}

func (rwt *spannerReadWriteTXN) UnregisterCounter(ctx context.Context, name string) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	// Delete the counter from the table.
	key := spanner.Key{name}
	if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
		spanner.Delete(tableRelationshipCounter, spanner.KeySetFromKeys(key)),
	}); err != nil {
		return fmt.Errorf(errUnableToDeleteCounter, err)
	}

	return nil
}

func (rwt *spannerReadWriteTXN) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	// Update the counter's count and revision in the table.
	updatedTimestampTime := computedAtRevision.(revisions.TimestampRevision).Time()

	mutation := spanner.Update(tableRelationshipCounter,
		[]string{colCounterName, colCounterCurrentCount, colCounterUpdatedAtTimestamp},
		[]any{name, value, updatedTimestampTime},
	)

	if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf(errUnableToUpdateCounter, err)
	}

	return nil
}

func (rwt *spannerReadWriteTXN) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	var rowCountChange int64
	for _, mutation := range mutations {
		txnMut, countChange, err := spannerMutation(ctx, mutation.Operation, mutation.Relationship)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
		rowCountChange += countChange

		if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{txnMut}); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func spannerMutation(
	ctx context.Context,
	operation tuple.UpdateOperation,
	rel tuple.Relationship,
) (txnMut *spanner.Mutation, countChange int64, err error) {
	switch operation {
	case tuple.UpdateOperationTouch:
		countChange = 1
		txnMut = spanner.InsertOrUpdate(tableRelationship, allRelationshipCols, upsertVals(rel))
	case tuple.UpdateOperationCreate:
		countChange = 1
		txnMut = spanner.Insert(tableRelationship, allRelationshipCols, upsertVals(rel))
	case tuple.UpdateOperationDelete:
		countChange = -1
		txnMut = spanner.Delete(tableRelationship, keyFromRelationship(rel))
	default:
		log.Ctx(ctx).Error().Msg("unknown operation type")
		err = fmt.Errorf("unknown mutation operation: %v", operation)
		return txnMut, countChange, err
	}

	return txnMut, countChange, err
}

func (rwt *spannerReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...dsoptions.DeleteOptionsOption) (uint64, bool, error) {
	numDeleted, limitReached, err := deleteWithFilter(ctx, rwt.spannerRWT, filter, opts...)
	if err != nil {
		return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return numDeleted, limitReached, nil
}

func deleteWithFilter(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter, opts ...dsoptions.DeleteOptionsOption) (uint64, bool, error) {
	delOpts := dsoptions.NewDeleteOptionsWithOptionsAndDefaults(opts...)
	var delLimit uint64
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		delLimit = *delOpts.DeleteLimit
		if delLimit > inLimit {
			return 0, false, spiceerrors.MustBugf("delete limit %d exceeds maximum of %d in spanner", delLimit, inLimit)
		}
	}

	var numDeleted int64
	if delLimit > 0 {
		nu, err := deleteWithFilterAndLimit(ctx, rwt, filter, delLimit)
		if err != nil {
			return 0, false, err
		}
		numDeleted = nu
	} else {
		nu, err := deleteWithFilterAndNoLimit(ctx, rwt, filter)
		if err != nil {
			return 0, false, err
		}

		numDeleted = nu
	}

	uintNumDeleted, err := safecast.Convert[uint64](numDeleted)
	if err != nil {
		return 0, false, spiceerrors.MustBugf("numDeleted was negative: %v", err)
	}

	if delLimit > 0 && uintNumDeleted == delLimit {
		return uintNumDeleted, true, nil
	}

	return uintNumDeleted, false, nil
}

func deleteWithFilterAndLimit(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter, delLimit uint64) (int64, error) {
	query := queryTuplesForDelete
	filteredQuery, err := applyFilterToQuery(query, filter)
	if err != nil {
		return -1, err
	}
	query = filteredQuery
	query = query.Limit(delLimit)

	sql, args, err := query.ToSql()
	if err != nil {
		return -1, err
	}

	mutations := make([]*spanner.Mutation, 0, delLimit)

	// Load the relationships to be deleted.
	iter := rwt.Query(ctx, statementFromSQL(sql, args))
	defer iter.Stop()

	if err := iter.Do(func(row *spanner.Row) error {
		var resourceObjectType string
		var resourceObjectID string
		var relation string
		var subjectObjectType string
		var subjectObjectID string
		var subjectRelation string

		err := row.Columns(
			&resourceObjectType,
			&resourceObjectID,
			&relation,
			&subjectObjectType,
			&subjectObjectID,
			&subjectRelation,
		)
		if err != nil {
			return err
		}

		nextRel := tuple.Relationship{
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
		}

		mutations = append(mutations, spanner.Delete(tableRelationship, keyFromRelationship(nextRel)))
		return nil
	}); err != nil {
		return -1, err
	}

	// Delete the relationships.
	if err := rwt.BufferWrite(mutations); err != nil {
		return -1, fmt.Errorf(errUnableToWriteRelationships, err)
	}

	return int64(len(mutations)), nil
}

func deleteWithFilterAndNoLimit(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter) (int64, error) {
	query := sql.Delete(tableRelationship)
	filteredQuery, err := applyFilterToQuery(query, filter)
	if err != nil {
		return -1, err
	}
	query = filteredQuery

	sql, args, err := query.ToSql()
	if err != nil {
		return -1, err
	}

	deleteStatement := statementFromSQL(sql, args)
	return rwt.Update(ctx, deleteStatement)
}

type builder[T any] interface {
	Where(pred any, args ...any) T
}

func applyFilterToQuery[T builder[T]](query T, filter *v1.RelationshipFilter) (T, error) {
	// Add clauses for the ResourceFilter
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
			return query, fmt.Errorf(errUnableToDeleteRelationships, err)
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

	return query, nil
}

func upsertVals(r tuple.Relationship) []any {
	key := keyFromRelationship(r)
	key = append(key, spanner.CommitTimestamp)
	key = append(key, caveatVals(r)...)

	if r.OptionalExpiration != nil {
		key = append(key, spanner.NullTime{Time: *r.OptionalExpiration, Valid: true})
	} else {
		key = append(key, nil)
	}
	return key
}

func keyFromRelationship(r tuple.Relationship) spanner.Key {
	return spanner.Key{
		r.Resource.ObjectType,
		r.Resource.ObjectID,
		r.Resource.Relation,
		r.Subject.ObjectType,
		r.Subject.ObjectID,
		r.Subject.Relation,
	}
}

func caveatVals(r tuple.Relationship) []any {
	if r.OptionalCaveat == nil {
		return []any{"", nil}
	}
	vals := []any{r.OptionalCaveat.CaveatName}
	if r.OptionalCaveat.Context != nil {
		vals = append(vals, spanner.NullJSON{Value: r.OptionalCaveat.Context, Valid: true})
	} else {
		vals = append(vals, nil)
	}
	return vals
}

func (rwt *spannerReadWriteTXN) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	mutations := make([]*spanner.Mutation, 0, len(newConfigs))
	for _, newConfig := range newConfigs {
		serialized, err := newConfig.MarshalVT()
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}

		mutations = append(mutations, spanner.InsertOrUpdate(
			tableNamespace,
			[]string{colNamespaceName, colNamespaceConfig, colTimestamp},
			[]any{newConfig.Name, serialized, spanner.CommitTimestamp},
		))

		// Track the buffered namespace write so we can return it from List methods
		// without attempting to read from Spanner (which doesn't see buffered writes)
		rwt.bufferedNamespaces[newConfig.Name] = newConfig
		// Remove from deleted set in case it was previously deleted in this transaction
		delete(rwt.deletedNamespaces, newConfig.Name)
	}

	if err := rwt.spannerRWT.BufferWrite(mutations); err != nil {
		return err
	}

	return nil
}

func (rwt *spannerReadWriteTXN) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	if len(nsNames) == 0 {
		return nil
	}

	namespaces, err := rwt.LegacyLookupNamespacesWithNames(ctx, nsNames)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	if len(namespaces) != len(nsNames) {
		expectedNamespaceNames := mapz.NewSet[string](nsNames...)
		for _, ns := range namespaces {
			expectedNamespaceNames.Delete(ns.Definition.Name)
		}

		return fmt.Errorf(errUnableToDeleteConfig, fmt.Errorf("namespaces not found: %v", expectedNamespaceNames.AsSlice()))
	}

	for _, nsName := range nsNames {
		if delOption == datastore.DeleteNamespacesAndRelationships {
			relFilter := &v1.RelationshipFilter{ResourceType: nsName}
			if _, _, err := deleteWithFilter(ctx, rwt.spannerRWT, relFilter); err != nil {
				return fmt.Errorf(errUnableToDeleteConfig, err)
			}
		}

		err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
			spanner.Delete(tableNamespace, spanner.KeySetFromKeys(spanner.Key{nsName})),
		})
		if err != nil {
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}

		// Remove from buffered namespaces and mark as deleted so List methods won't return it
		delete(rwt.bufferedNamespaces, nsName)
		rwt.deletedNamespaces[nsName] = struct{}{}
	}

	return nil
}

func (rwt *spannerReadWriteTXN) SchemaWriter() (datastore.SchemaWriter, error) {
	// Wrap the transaction with an unexported schema writer
	writer := &spannerSchemaWriter{rwt: rwt}
	return schemaadapter.NewSchemaWriter(writer, writer, rwt.schemaMode), nil
}

// spannerSchemaWriter wraps a spannerReadWriteTXN and implements DualSchemaWriter.
// This prevents direct access to schema write methods from the transaction.
type spannerSchemaWriter struct {
	rwt *spannerReadWriteTXN
}

// WriteStoredSchema implements datastore.SingleStoreSchemaWriter
func (w *spannerSchemaWriter) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	// Create an executor that uses the current transaction
	executor := newSpannerChunkedBytesExecutor(w.rwt.spannerRWT)

	// Use the shared schema reader/writer to write the schema
	// Spanner uses delete-and-insert mode so no transaction ID provider is needed
	noTxID := func(ctx context.Context) any { return common.NoTransactionID[any](ctx) }
	if err := w.rwt.schemaReaderWriter.WriteSchema(ctx, schema, executor, noTxID); err != nil {
		return err
	}

	// Write the schema hash to the schema_revision table for fast lookups
	if err := w.writeSchemaHash(ctx, schema); err != nil {
		return fmt.Errorf("failed to write schema hash: %w", err)
	}

	return nil
}

// writeSchemaHash writes the schema hash to the schema_revision table
func (w *spannerSchemaWriter) writeSchemaHash(ctx context.Context, schema *core.StoredSchema) error {
	v1 := schema.GetV1()
	if v1 == nil {
		return fmt.Errorf("unsupported schema version: %d", schema.Version)
	}

	// Use InsertOrUpdate mutation to upsert the schema hash
	mutation := spanner.InsertOrUpdate(
		tableSchemaRevision,
		[]string{"name", "schema_hash", "timestamp"},
		[]any{"current", []byte(v1.SchemaHash), spanner.CommitTimestamp},
	)

	if err := w.rwt.spannerRWT.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("failed to buffer schema hash write: %w", err)
	}

	return nil
}

// writeSchemaHashFromDefinitions writes the schema hash computed from the given definitions
func (rwt *spannerReadWriteTXN) writeSchemaHashFromDefinitions(ctx context.Context, namespaces []datastore.RevisionedNamespace, caveats []datastore.RevisionedCaveat) error {
	// Build schema definitions list
	definitions := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
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
		return fmt.Errorf("failed to generate schema: %w", err)
	}

	// Compute schema hash (SHA256)
	hash := sha256.Sum256([]byte(schemaText))
	schemaHash := hex.EncodeToString(hash[:])

	// Use InsertOrUpdate mutation to upsert the schema hash
	mutation := spanner.InsertOrUpdate(
		tableSchemaRevision,
		[]string{"name", "schema_hash", "timestamp"},
		[]any{"current", []byte(schemaHash), spanner.CommitTimestamp},
	)

	if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("failed to buffer schema hash write: %w", err)
	}

	return nil
}

// ReadStoredSchema implements datastore.SingleStoreSchemaReader to satisfy DualSchemaReader interface requirements
func (w *spannerSchemaWriter) ReadStoredSchema(ctx context.Context) (*core.StoredSchema, error) {
	// Create an executor that uses the current write transaction for reads
	// This ensures we read within the same transaction, avoiding "transaction already committed" errors
	executor := newSpannerChunkedBytesExecutor(w.rwt.spannerRWT)

	// Use the shared schema reader/writer to read the schema
	// Pass empty string for transaction reads to bypass cache
	return w.rwt.schemaReaderWriter.ReadSchema(ctx, executor, nil, datastore.NoSchemaHashInTransaction)
}

// LegacyWriteNamespaces delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	return w.rwt.LegacyWriteNamespaces(ctx, newConfigs...)
}

// LegacyDeleteNamespaces delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	return w.rwt.LegacyDeleteNamespaces(ctx, nsNames, delOption)
}

// LegacyLookupNamespacesWithNames delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedDefinition[*core.NamespaceDefinition], error) {
	return w.rwt.LegacyLookupNamespacesWithNames(ctx, nsNames)
}

// LegacyWriteCaveats delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	return w.rwt.LegacyWriteCaveats(ctx, caveats)
}

// LegacyDeleteCaveats delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	return w.rwt.LegacyDeleteCaveats(ctx, names)
}

// LegacyReadCaveatByName delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	return w.rwt.LegacyReadCaveatByName(ctx, name)
}

// LegacyListAllCaveats delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return w.rwt.LegacyListAllCaveats(ctx)
}

// LegacyLookupCaveatsWithNames delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyLookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	return w.rwt.LegacyLookupCaveatsWithNames(ctx, names)
}

// LegacyReadNamespaceByName delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	return w.rwt.LegacyReadNamespaceByName(ctx, nsName)
}

// LegacyListAllNamespaces delegates to the underlying transaction
func (w *spannerSchemaWriter) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	return w.rwt.LegacyListAllNamespaces(ctx)
}

// WriteLegacySchemaHashFromDefinitions implements datastore.LegacySchemaHashWriter
func (w *spannerSchemaWriter) WriteLegacySchemaHashFromDefinitions(ctx context.Context, namespaces []datastore.RevisionedNamespace, caveats []datastore.RevisionedCaveat) error {
	return w.rwt.writeSchemaHashFromDefinitions(ctx, namespaces, caveats)
}

func (rwt *spannerReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	var numLoaded uint64
	var rel *tuple.Relationship
	var err error
	for rel, err = iter.Next(ctx); err == nil && rel != nil; rel, err = iter.Next(ctx) {
		txnMut, _, err := spannerMutation(ctx, tuple.UpdateOperationCreate, *rel)
		if err != nil {
			return 0, fmt.Errorf(errUnableToBulkLoadRelationships, err)
		}
		numLoaded++

		if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{txnMut}); err != nil {
			return 0, fmt.Errorf(errUnableToBulkLoadRelationships, err)
		}
	}

	if err != nil {
		return 0, fmt.Errorf(errUnableToBulkLoadRelationships, err)
	}

	return numLoaded, nil
}

var (
	_ datastore.ReadWriteTransaction = (*spannerReadWriteTXN)(nil)
	_ datastore.LegacySchemaWriter   = (*spannerReadWriteTXN)(nil)
	_ datastore.DualSchemaWriter     = (*spannerSchemaWriter)(nil)
	_ datastore.DualSchemaReader     = (*spannerSchemaWriter)(nil)
)
