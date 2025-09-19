package otelconv

// This file contains constants for telemetry instrumentation that follow the OpenTelemetry
// Semantic Conventions for naming. The naming conventions adhere to the guidelines specified at:
// https://opentelemetry.io/docs/specs/semconv/general/naming/
//
// The events and attributes defined here are used throughout the SpiceDB codebase to provide
// a consistent naming scheme for spans, events, attributes, etc.
//
// IMPORTANT: When adding a new _custom_ name, consider the following conventions:
//  - Prefix all names with "spicedb.internal" or "spicedb.external" to identify application domain:
//		- Use "external" prefix for events/attributes potentially relevant to end users
//		- Use "internal" prefix for events/attributes used only internally i.e. debugging
//  - Use lowercase letters and numbers only
//  - Order name parts from most general to most specific
//  - For events, use noun.verb and the verb in past tense to describe the action
//  - For attributes, use noun.descriptor format to describe the data
//  - Be consistent with existing naming patterns
//  - Before adding a new name, search for any existing names that serve the same purpose
//  - Names should be chosen to minimize cardinality and maintain a reasonable set of possible values overall
//
//  Here's the format for defining new custom event/attribute names:
//    spicedb.{domain}.{subdomain}.{action_past_tense|descriptor}

// Custom event names
const (
	EventCaveatsNamesCollected = "spicedb.internal.caveats.names_collected"
	EventCaveatsLookedUp       = "spicedb.internal.caveats.lookup_completed"

	EventRelationshipsMutationsValidated     = "spicedb.internal.service.relationships.mutations_validated"
	EventRelationshipsReadWriteExecuted      = "spicedb.internal.service.relationships.readwrite_executed"
	EventRelationshipsPreconditionsValidated = "spicedb.internal.service.relationships.preconditions.validated"
	EventRelationshipsUpdatesValidated       = "spicedb.internal.service.relationships.updates.validated"
	EventRelationshipsWritten                = "spicedb.internal.service.relationships.written"

	EventDatastoreColumnsSelected     = "spicedb.internal.datastore.shared.columns_selected"
	EventDatastoreIteratorCreate      = "spicedb.internal.datastore.shared.iterator_created"
	EventDatastoreExecuteIssued       = "spicedb.internal.datastore.shared.execute_issued"
	EventDatastoreExecuteStarted      = "spicedb.internal.datastore.shared.execute_started"
	EventDatastoreRowsFirstReturned   = "spicedb.internal.datastore.shared.first_row_returned"
	EventDatastoreRowsFirstScanned    = "spicedb.internal.datastore.shared.first_row_scanned"
	EventDatastoreRelationshipsLoaded = "spicedb.internal.datastore.shared.relationships_loaded"

	EventDatastoreSpannerQueryIssued      = "spicedb.internal.datastore.spanner.query_issued"
	EventDatastoreSpannerIteratorStarted  = "spicedb.internal.datastore.spanner.iterator_started"
	EventDatastoreSpannerIteratorFinished = "spicedb.internal.datastore.spanner.iterator_finished"

	EventDatastoreMySQLTransactionValidated = "spicedb.internal.datastore.mysql.transaction_validated"

	EventDatastoreRevisionsCacheReturned = "spicedb.internal.datastore.revisions.cache_returned"
	EventDatastoreRevisionsComputed      = "spicedb.internal.datastore.revisions.computed"

	EventDispatchLookupResources3                          = "spicedb.internal.dispatch.lookupresources3"
	EventDispatchLR3UnlimitedResults                       = "spicedb.internal.dispatch.lookupresources3.unlimited_results"
	EventDispatchLR3UnlimitedResultsDirectSubjects         = "spicedb.internal.dispatch.lookupresources3.unlimited_results.direct_subjects"
	EventDispatchLookupResources3EntrypointsIter           = "spicedb.internal.dispatch.lookupresources3.entrypoints_iter"
	EventDispatchLookupResources3RelationEntrypoint        = "spicedb.internal.dispatch.lookupresources3.relation_entrypoint"
	EventDispatchLookupResources3ArrowEntrypoint           = "spicedb.internal.dispatch.lookupresources3.arrow_entrypoint"
	EventDispatchLookupResources3RelationshipsIterProducer = "spicedb.internal.dispatch.lookupresources3.relationships_iter"
	EventDispatchLookupResources3RelationshipsIterMapper   = "spicedb.internal.dispatch.lookupresources3.relationships_iter.mapper"
	EventDispatchLookupResources3CheckedDispatchIter       = "spicedb.internal.dispatch.lookupresources3.checked_dispatch_iter"
	EventDispatchLookupResources3DispatchIter              = "spicedb.internal.dispatch.lookupresources3.checked_dispatch_iter"
	EventDispatchLookupResources3ConcurrentEntrypointsIter = "spicedb.internal.dispatch.lookupresources3.concurrent_entrypoints_iter"
)

// OpenTelemetry custom attribute names
const (
	AttrCaveatsNames      = "spicedb.internal.caveats.names"
	AttrCaveatsOperations = "spicedb.internal.caveats.operations"
	AttrDispatchCached    = "spicedb.internal.dispatch.cached"

	AttrGraphSourceResourceTypeNamespace   = "spicedb.internal.graph.source_resource_type.namespace"
	AttrGraphSourceResourceTypeRelation    = "spicedb.internal.graph.source_resource_type.relation"
	AttrGraphSubjectsFilterSubjectType     = "spicedb.internal.graph.subject_filter.subject_type"
	AttrGraphSubjectsFilterSubjectIdsCount = "spicedb.internal.graph.subject_filter.subject_ids_count"
	AttrGraphFoundResourcesCount           = "spicedb.internal.graph.found_resources_count"
	AttrGraphResourceIDCount               = "spicedb.internal.graph.resource_id_count"

	AttrDispatchResourceType       = "spicedb.internal.dispatch.resource_type"
	AttrDispatchResourceIds        = "spicedb.internal.dispatch.resource_ids"
	AttrDispatchResourceRelation   = "spicedb.internal.dispatch.resource_relation"
	AttrDispatchSubject            = "spicedb.internal.dispatch.subject"
	AttrDispatchNodeID             = "spicedb.internal.dispatch.node_id"
	AttrDispatchStart              = "spicedb.internal.dispatch.start"
	AttrDispatchSubjectType        = "spicedb.internal.dispatch.subject_type"
	AttrDispatchSubjectIDs         = "spicedb.internal.dispatch.subject_ids"
	AttrDispatchSubjectRelation    = "spicedb.internal.dispatch.subject_relation"
	AttrDispatchTerminalSubject    = "spicedb.internal.dispatch.terminal_subject"
	AttrDispatchLREntrypoint       = "spicedb.internal.dispatch.lookupresources3.entrypoint"
	AttrDispatchLRConcurrencyLimit = "spicedb.internal.dispatch.lookupresources3.concurrency_limit"
	AttrDispatchLREntrypointCount  = "spicedb.internal.dispatch.lookupresources3.entrypoint_count"
	AttrDispatchCursorLimit        = "spicedb.internal.dispatch.lookup.cursor_limit"

	AttrDatastoreNames              = "spicedb.internal.datastore.shared.names"
	AttrDatastoreValue              = "spicedb.internal.datastore.shared.value"
	AttrDatastoreRevision           = "spicedb.internal.datastore.shared.revision"
	AttrDatastoreMutations          = "spicedb.internal.datastore.shared.mutations"
	AttrDatastoreResourceType       = "spicedb.internal.datastore.shared.resource_type"
	AttrDatastoreResourceRelation   = "spicedb.internal.datastore.shared.resource_relation"
	AttrDatastoreQueryShape         = "spicedb.internal.datastore.shared.query_shape"
	AttrDatastoreSubjectType        = "spicedb.internal.datastore.shared.subject_type"
	AttrDatastoreColumnCount        = "spicedb.internal.datastore.shared.column_count"
	AttrDatastoreRelationshipsCount = "spicedb.internal.datastore.shared.relationship_count"

	AttrDatastoreSpannerAPI       = "spicedb.internal.datastore.spanner.api"
	AttrDatastoreSpannerTable     = "spicedb.internal.datastore.spanner.table"
	AttrDatastoreSpannerKey       = "spicedb.internal.datastore.spanner.key"
	AttrDatastoreSpannerColumns   = "spicedb.internal.datastore.spanner.columns"
	AttrDatastoreSpannerStatement = "spicedb.internal.datastore.spanner.statement"
	AttrDatastoreRelationCount    = "spicedb.internal.datastore.spanner.relation_count"
	AttrDatastoreNamespaceCount   = "spicedb.internal.datastore.spanner.namespace_count"

	AttrDatastorePostgresLogLevel = "spicedb.internal.datastore.postgres.log_level"

	AttrDatastoreMySQLTransactionFresh   = "spicedb.internal.datastore.mysql.transaction_fresh"
	AttrDatastoreMySQLTransactionUnknown = "spicedb.internal.datastore.mysql.transaction_unknown"

	AttrIteratorItemCount = "spicedb.internal.iterator.item_count"

	AttrTestKey    = "spicedb.internal.test.key"
	AttrTestNumber = "spicedb.internal.test.number"
)
