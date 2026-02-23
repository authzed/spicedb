package v1

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	caveatsimpl "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// checkPermissionWithQueryPlan executes a permission check using the query plan API.
// This builds an iterator tree from the schema and executes it against the datastore.
func (ps *permissionServer) checkPermissionWithQueryPlan(ctx context.Context, req *v1.CheckPermissionRequest) (*v1.CheckPermissionResponse, error) {
	atRevision, checkedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	dl := datalayer.MustFromContext(ctx)
	reader := dl.SnapshotReader(atRevision)

	// Load all namespace and caveat definitions to build the schema
	// TODO: Better schema caching
	sr, err := reader.ReadSchema()
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

	// Build iterator tree from schema
	// TODO: Better iterator caching
	it, err := query.BuildIteratorFromSchema(fullSchema, req.Resource.ObjectType, req.Permission)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Apply basic optimizations to the iterator tree
	it, _, err = query.ApplyOptimizations(it, query.StaticOptimizations)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Parse caveat context if provided
	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Create query context with optional tracing
	qctx := &query.Context{
		Context:       ctx,
		Executor:      query.LocalExecutor{},
		Reader:        reader,
		CaveatContext: caveatContext,
		CaveatRunner:  caveatsimpl.NewCaveatRunner(ps.config.CaveatTypeSet),
	}

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

	pathSeq, err := qctx.Check(it, []query.Object{resource}, subject)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Collect results and convert to response
	permissionship, partialCaveat, err := convertPathsToPermissionship(pathSeq)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	resp := &v1.CheckPermissionResponse{
		CheckedAt:         checkedAt,
		Permissionship:    permissionship,
		PartialCaveatInfo: partialCaveat,
	}

	return resp, nil
}

// convertPathsToPermissionship iterates over paths and determines the permissionship result.
// Returns the first path's permissionship status, as any path indicates access.
func convertPathsToPermissionship(pathSeq query.PathSeq) (v1.CheckPermissionResponse_Permissionship, *v1.PartialCaveatInfo, error) {
	// Iterate over paths to find the first valid result
	for path, err := range pathSeq {
		if err != nil {
			return v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED, nil, err
		}

		// Found a path - determine permissionship based on caveat presence
		if path.Caveat != nil {
			// TODO: Extract missing required context from caveat expression
			return v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION, &v1.PartialCaveatInfo{
				MissingRequiredContext: []string{},
			}, nil
		}

		// Path exists without caveat - has permission
		return v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, nil, nil
	}

	// No paths found - no permission
	return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, nil, nil
}
