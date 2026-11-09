package v1

import (
	"cmp"
	"context"
	"slices"

	"github.com/ccoveille/go-safecast/v2"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	caveatsimpl "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/queryplan"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/tuple"
)

// checkPermissionWithQueryPlan executes a permission check using the query plan API.
// This builds an iterator tree from the schema and executes it against the datastore.
func (ps *permissionServer) checkPermissionWithQueryPlan(
	ctx context.Context,
	req *v1.CheckPermissionRequest,
) (*v1.CheckPermissionResponse, error) {
	atRevision, schemaHash, checkedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	dl := datalayer.MustFromContext(ctx)
	reader := dl.SnapshotReader(atRevision, schemaHash)

	// Read the schema at the request revision; single-store readers own the
	// converted view reused below.
	sr, err := reader.ReadSchema(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Validate resource permission and subject relation through the same schema
	// reader as native before planning, preserving public invalid-request errors.
	if err := namespace.CheckNamespaceAndRelations(ctx, []namespace.TypeAndRelationToCheck{
		{
			NamespaceName: req.Resource.ObjectType,
			RelationName:  req.Permission,
			AllowEllipsis: false,
		},
		{
			NamespaceName: req.Subject.Object.ObjectType,
			RelationName:  normalizeSubjectRelation(req.Subject),
			AllowEllipsis: true,
		},
	}, sr); err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	maximumAPIDepth, err := safecast.Convert[int32](ps.config.MaximumAPIDepth)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Planning owns profile capabilities, root cache probes and execution setup.
	// The API retains validation above and request-context caveat evaluation below.
	planner := queryplan.Planner{Profile: ps.config.QueryProfile, Signals: ps.querySignals}
	runner := caveatsimpl.NewCaveatRunner(ps.config.CaveatTypeSet)
	path, err := planner.Check(ctx, queryplan.CheckRequest{
		Request: queryplan.Request{
			Schema:        sr,
			Revision:      atRevision,
			SchemaHash:    schemaHash,
			CaveatContext: caveatContext,
		},
		Permission: req.Permission,
		Resource:   query.Object{ObjectType: req.Resource.ObjectType, ObjectID: req.Resource.ObjectId},
		Subject: query.ObjectAndRelation{
			ObjectType: req.Subject.Object.ObjectType,
			ObjectID:   req.Subject.Object.ObjectId,
			Relation:   normalizeSubjectRelation(req.Subject),
		},
		Dispatcher:        ps.dispatch,
		Reader:            reader,
		CaveatRunner:      runner,
		MaximumDepth:      maximumAPIDepth,
		DispatchChunkSize: ps.config.DispatchChunkSize,
	})
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// Cached and executed paths remain expressions until evaluated with this
	// request's context. Intermediate dispatch results retain conditional paths.
	permissionship, partialCaveat, err := evaluateCheckPath(ctx, path, caveatContext, sr, runner)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	return &v1.CheckPermissionResponse{
		CheckedAt:         checkedAt,
		Permissionship:    permissionship,
		PartialCaveatInfo: partialCaveat,
	}, nil
}

// evaluateCheckPath resolves the complete expression at the public boundary because
// intermediate dispatch results must retain conditional paths for their parent reducers.
func evaluateCheckPath(
	ctx context.Context,
	path *query.Path,
	values map[string]any,
	reader datalayer.SchemaReader,
	runner *caveatsimpl.CaveatRunner,
) (v1.CheckPermissionResponse_Permissionship, *v1.PartialCaveatInfo, error) {
	if path == nil || path.Caveat == nil {
		permission, partial := convertPathToPermissionship(path)
		return permission, partial, nil
	}

	result, err := runner.RunCaveatExpression(ctx, path.Caveat, values, reader, caveatsimpl.RunCaveatExpressionNoDebugging)
	if err != nil {
		return 0, nil, err
	}

	if result.IsPartial() {
		missing, err := result.MissingVarNames()
		if err != nil {
			return 0, nil, err
		}

		slices.Sort(missing)
		return v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
			&v1.PartialCaveatInfo{MissingRequiredContext: missing}, nil
	}

	if result.Value() {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, nil, nil
	}

	return v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, nil, nil
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
func (ps *permissionServer) lookupResourcesWithQueryPlan(
	req *v1.LookupResourcesRequest,
	resp v1.PermissionsService_LookupResourcesServer,
) error {
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

	queryParams := queryopt.RequestParams{
		Operation:       query.OperationIterResources,
		SubjectType:     req.Subject.Object.ObjectType,
		SubjectRelation: normalizeSubjectRelation(req.Subject),
	}

	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	planner := queryplan.Planner{Profile: ps.config.QueryProfile, Signals: ps.querySignals}
	plan, err := planner.FromSchema(ctx, queryplan.Request{
		Schema:        sr,
		Revision:      atRevision,
		SchemaHash:    schemaHash,
		Params:        queryParams,
		CaveatContext: caveatContext,
	}, req.ResourceObjectType, req.Permission)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	defer plan.Flush()

	maximumAPIDepth, err := safecast.Convert[int32](ps.config.MaximumAPIDepth)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	planContext := dispatch.NewPlanContext(atRevision.String(), schemaHash, caveatContext, maximumAPIDepth, 0)
	qctx := dispatch.NewQueryContext(
		ctx,
		ps.dispatch,
		planContext,
		query.NewQueryDatastoreReader(reader),
		caveatsimpl.NewCaveatRunner(ps.config.CaveatTypeSet),
		ps.config.DispatchChunkSize,
		plan.Options...,
	)

	// Build the subject from the request
	subject := query.ObjectAndRelation{
		ObjectType: req.Subject.Object.ObjectType,
		ObjectID:   req.Subject.Object.ObjectId,
		Relation:   normalizeSubjectRelation(req.Subject),
	}

	// Iterate over resources accessible from this subject
	pathSeq, err := qctx.IterResources(plan.Iterator, subject, query.NoObjectFilter())
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

	return nil
}

// lookupSubjectsWithQueryPlan executes a LookupSubjects call using the query plan API.
// It builds an iterator tree from the schema and calls IterSubjects to stream results.
func (ps *permissionServer) lookupSubjectsWithQueryPlan(
	req *v1.LookupSubjectsRequest,
	resp v1.PermissionsService_LookupSubjectsServer,
) error {
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

	queryParams := queryopt.RequestParams{
		Operation:       query.OperationIterSubjects,
		SubjectType:     req.SubjectObjectType,
		SubjectRelation: cmp.Or(req.OptionalSubjectRelation, tuple.Ellipsis),
	}

	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	planner := queryplan.Planner{Profile: ps.config.QueryProfile, Signals: ps.querySignals}
	plan, err := planner.FromSchema(ctx, queryplan.Request{
		Schema:        sr,
		Revision:      atRevision,
		SchemaHash:    schemaHash,
		Params:        queryParams,
		CaveatContext: caveatContext,
	}, req.Resource.ObjectType, req.Permission)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	defer plan.Flush()

	maximumAPIDepth, err := safecast.Convert[int32](ps.config.MaximumAPIDepth)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	planContext := dispatch.NewPlanContext(atRevision.String(), schemaHash, caveatContext, maximumAPIDepth, 0)
	qctx := dispatch.NewQueryContext(
		ctx,
		ps.dispatch,
		planContext,
		query.NewQueryDatastoreReader(reader),
		caveatsimpl.NewCaveatRunner(ps.config.CaveatTypeSet),
		ps.config.DispatchChunkSize,
		plan.Options...,
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
	pathSeq, err := qctx.IterSubjects(plan.Iterator, resource, subjectFilter)
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

	return nil
}
