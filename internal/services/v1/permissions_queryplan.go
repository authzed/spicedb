package v1

import (
	"cmp"
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	caveatsimpl "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// checkPermissionWithQueryPlan executes a permission check using the query plan API.
// This builds an iterator tree from the schema and executes it against the datastore.
func (ps *permissionServer) checkPermissionWithQueryPlan(ctx context.Context, req *v1.CheckPermissionRequest) (*v1.CheckPermissionResponse, error) {
	atRevision, schemaHash, checkedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	dl := datalayer.MustFromContext(ctx)
	reader := dl.SnapshotReader(atRevision, schemaHash)

	// Load all namespace and caveat definitions to build the schema
	// TODO: Better schema caching
	sr, err := reader.ReadSchema(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	namespaces, err := sr.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	caveats, err := sr.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Build schema from definitions
	fullSchema, err := schema.BuildSchemaFromDefinitions(
		datastore.DefinitionsOf(namespaces),
		datastore.DefinitionsOf(caveats),
	)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Build and optimize the outline, then compile to an iterator tree.
	// TODO: Better outline caching
	co, err := query.BuildOutlineFromSchema(fullSchema, req.Resource.ObjectType, req.Permission)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	queryParams := queryopt.RequestParams{
		Operation:       query.OperationCheck,
		SubjectType:     req.Subject.Object.ObjectType,
		SubjectRelation: req.Subject.OptionalRelation,
	}
	optimized, err := queryopt.ApplyOptimizations(co, queryopt.OptimizersForRequest(queryParams), queryParams)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Apply count-based advisor if stats have been accumulated from prior requests.
	optimized, err = ps.queryPlanMetadata.ApplyAdvisor(optimized)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	it, err := optimized.Compile()
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Parse caveat context if provided
	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Create count observer to track query execution statistics
	countObserver := query.NewCountObserver()

	// Create query context backed by a DispatchExecutor.
	planContext := dispatch.NewPlanContext(atRevision.String(), schemaHash, caveatContext, int(ps.config.MaximumAPIDepth), 0)
	qctx := dispatch.NewQueryContext(
		ctx,
		ps.dispatch,
		planContext,
		query.NewQueryDatastoreReader(reader),
		caveatsimpl.NewCaveatRunner(ps.config.CaveatTypeSet),
		ps.config.DispatchChunkSize,
		query.WithObserver(countObserver),
	)

	// Execute the check
	resource := query.Object{
		ObjectType: req.Resource.ObjectType,
		ObjectID:   req.Resource.ObjectId,
	}

	subject := query.ObjectAndRelation{
		ObjectType: req.Subject.Object.ObjectType,
		ObjectID:   req.Subject.Object.ObjectId,
		Relation:   normalizeSubjectRelation(req.Subject),
	}

	path, err := qctx.Check(it, resource, subject)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Convert result path to response
	permissionship, partialCaveat := convertPathToPermissionship(path)

	// Merge count statistics into metadata after query completes
	countStats := countObserver.GetStats()
	ps.queryPlanMetadata.MergeCountStats(countStats)

	resp := &v1.CheckPermissionResponse{
		CheckedAt:         checkedAt,
		Permissionship:    permissionship,
		PartialCaveatInfo: partialCaveat,
	}

	return resp, nil
}

// convertPathToPermissionship converts a single check result path to a permissionship response.
// A nil path means no permission; a non-nil path means permission (conditional if it has a caveat).
func convertPathToPermissionship(path *query.Path) (v1.CheckPermissionResponse_Permissionship, *v1.PartialCaveatInfo) {
	if path == nil {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, nil
	}

	if path.Caveat != nil {
		// TODO: Extract missing required context from caveat expression
		return v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION, &v1.PartialCaveatInfo{
			MissingRequiredContext: []string{},
		}
	}

	return v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, nil
}

// lookupResourcesWithQueryPlan executes a LookupResources call using the query plan API.
// It builds an iterator tree from the schema and calls IterResources to stream results.
func (ps *permissionServer) lookupResourcesWithQueryPlan(req *v1.LookupResourcesRequest, resp v1.PermissionsService_LookupResourcesServer) error {
	ctx := resp.Context()

	atRevision, schemaHash, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	dl := datalayer.MustFromContext(ctx)
	reader := dl.SnapshotReader(atRevision, schemaHash)

	// Load schema
	sr, err := reader.ReadSchema(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	namespaces, err := sr.ListAllTypeDefinitions(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	caveats, err := sr.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	fullSchema, err := schema.BuildSchemaFromDefinitions(
		datastore.DefinitionsOf(namespaces),
		datastore.DefinitionsOf(caveats),
	)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	// Build and compile the iterator tree for the requested resource type and permission
	co, err := query.BuildOutlineFromSchema(fullSchema, req.ResourceObjectType, req.Permission)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	queryParams := queryopt.RequestParams{
		Operation:       query.OperationIterResources,
		SubjectType:     req.Subject.Object.ObjectType,
		SubjectRelation: normalizeSubjectRelation(req.Subject),
	}
	optimized, err := queryopt.ApplyOptimizations(co, queryopt.OptimizersForRequest(queryParams), queryParams)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	// Apply count-based advisor if stats have been accumulated from prior requests.
	optimized, err = ps.queryPlanMetadata.ApplyAdvisor(optimized)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	it, err := optimized.Compile()
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	// Parse caveat context if provided
	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	countObserver := query.NewCountObserver()

	planContext := dispatch.NewPlanContext(atRevision.String(), schemaHash, caveatContext, int(ps.config.MaximumAPIDepth), 0)
	qctx := dispatch.NewQueryContext(
		ctx,
		ps.dispatch,
		planContext,
		query.NewQueryDatastoreReader(reader),
		caveatsimpl.NewCaveatRunner(ps.config.CaveatTypeSet),
		ps.config.DispatchChunkSize,
		query.WithObserver(countObserver),
	)

	// Build the subject from the request
	subject := query.ObjectAndRelation{
		ObjectType: req.Subject.Object.ObjectType,
		ObjectID:   req.Subject.Object.ObjectId,
		Relation:   normalizeSubjectRelation(req.Subject),
	}

	// Iterate over resources accessible from this subject
	pathSeq, err := qctx.IterResources(it, subject, query.NoObjectFilter())
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	var totalCountPublished uint64
	for path, err := range pathSeq {
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		permissionship := v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
		var partial *v1.PartialCaveatInfo
		if path.Caveat != nil {
			permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION
			partial = &v1.PartialCaveatInfo{
				MissingRequiredContext: []string{},
			}
		}

		if err := resp.Send(&v1.LookupResourcesResponse{
			LookedUpAt:        revisionReadAt,
			ResourceObjectId:  path.Resource.ObjectID,
			Permissionship:    permissionship,
			PartialCaveatInfo: partial,
		}); err != nil {
			return err
		}
		totalCountPublished++
	}

	countStats := countObserver.GetStats()
	ps.queryPlanMetadata.MergeCountStats(countStats)

	return nil
}

// lookupSubjectsWithQueryPlan executes a LookupSubjects call using the query plan API.
// It builds an iterator tree from the schema and calls IterSubjects to stream results.
func (ps *permissionServer) lookupSubjectsWithQueryPlan(req *v1.LookupSubjectsRequest, resp v1.PermissionsService_LookupSubjectsServer) error {
	ctx := resp.Context()

	atRevision, schemaHash, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	dl := datalayer.MustFromContext(ctx)
	reader := dl.SnapshotReader(atRevision, schemaHash)

	// Load schema
	sr, err := reader.ReadSchema(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	namespaces, err := sr.ListAllTypeDefinitions(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	caveats, err := sr.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	fullSchema, err := schema.BuildSchemaFromDefinitions(
		datastore.DefinitionsOf(namespaces),
		datastore.DefinitionsOf(caveats),
	)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	// Build and compile the iterator tree for the resource type and permission
	co, err := query.BuildOutlineFromSchema(fullSchema, req.Resource.ObjectType, req.Permission)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	queryParams := queryopt.RequestParams{
		Operation:       query.OperationIterSubjects,
		SubjectType:     req.SubjectObjectType,
		SubjectRelation: cmp.Or(req.OptionalSubjectRelation, tuple.Ellipsis),
	}
	optimized, err := queryopt.ApplyOptimizations(co, queryopt.OptimizersForRequest(queryParams), queryParams)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	// Apply count-based advisor if stats have been accumulated from prior requests.
	optimized, err = ps.queryPlanMetadata.ApplyAdvisor(optimized)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	it, err := optimized.Compile()
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	// Parse caveat context if provided
	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	countObserver := query.NewCountObserver()

	planContext := dispatch.NewPlanContext(atRevision.String(), schemaHash, caveatContext, int(ps.config.MaximumAPIDepth), 0)
	qctx := dispatch.NewQueryContext(
		ctx,
		ps.dispatch,
		planContext,
		query.NewQueryDatastoreReader(reader),
		caveatsimpl.NewCaveatRunner(ps.config.CaveatTypeSet),
		ps.config.DispatchChunkSize,
		query.WithObserver(countObserver),
	)

	// Build the resource object from the request
	resource := query.Object{
		ObjectType: req.Resource.ObjectType,
		ObjectID:   req.Resource.ObjectId,
	}

	// Build a subject type filter from the request (type + optional relation)
	subjectFilter := query.ObjectType{
		Type:        req.SubjectObjectType,
		Subrelation: cmp.Or(req.OptionalSubjectRelation, tuple.Ellipsis),
	}

	// Iterate over subjects that have access to this resource
	pathSeq, err := qctx.IterSubjects(it, resource, subjectFilter)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	var totalCountPublished uint64
	for path, err := range pathSeq {
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		permissionship := v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
		var partial *v1.PartialCaveatInfo
		if path.Caveat != nil {
			permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION
			partial = &v1.PartialCaveatInfo{
				MissingRequiredContext: []string{},
			}
		}

		subject := &v1.ResolvedSubject{
			SubjectObjectId:   path.Subject.ObjectID,
			Permissionship:    permissionship,
			PartialCaveatInfo: partial,
		}

		if err := resp.Send(&v1.LookupSubjectsResponse{
			LookedUpAt:         revisionReadAt,
			Subject:            subject,
			SubjectObjectId:    path.Subject.ObjectID, // Deprecated
			Permissionship:     permissionship,        // Deprecated
			PartialCaveatInfo:  partial,               // Deprecated
			ExcludedSubjectIds: []string{},            // Deprecated; query plan does not compute exclusions
			ExcludedSubjects:   []*v1.ResolvedSubject{},
		}); err != nil {
			return err
		}
		totalCountPublished++
	}

	countStats := countObserver.GetStats()
	ps.queryPlanMetadata.MergeCountStats(countStats)

	return nil
}
