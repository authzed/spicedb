package v1

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	dispatchpkg "github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/perfinsights"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/telemetry"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func (ps *permissionServer) rewriteError(ctx context.Context, err error) error {
	return shared.RewriteError(ctx, err, &shared.ConfigForErrors{
		MaximumAPIDepth: ps.config.MaximumAPIDepth,
	})
}

func (ps *permissionServer) rewriteErrorWithOptionalDebugTrace(ctx context.Context, err error, debugTrace *v1.DebugInformation) error {
	return shared.RewriteError(ctx, err, &shared.ConfigForErrors{
		MaximumAPIDepth: ps.config.MaximumAPIDepth,
		DebugTrace:      debugTrace,
	})
}

func (ps *permissionServer) CheckPermission(ctx context.Context, req *v1.CheckPermissionRequest) (*v1.CheckPermissionResponse, error) {
	perfinsights.SetInContext(ctx, func() perfinsights.APIShapeLabels {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     req.GetResource().GetObjectType(),
			perfinsights.ResourceRelationLabel: req.GetPermission(),
			perfinsights.SubjectTypeLabel:      req.GetSubject().GetObject().GetObjectType(),
			perfinsights.SubjectRelationLabel:  req.GetSubject().GetOptionalRelation(),
		}
	})

	telemetry.LogicalChecks.Inc()

	if ps.config.ExperimentalQueryPlan {
		return ps.checkPermissionWithQueryPlan(ctx, req)
	}

	atRevision, checkedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	caveatContext, err := GetCaveatContext(ctx, req.GetContext(), ps.config.MaxCaveatContextSize)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	if err := namespace.CheckNamespaceAndRelations(ctx,
		[]namespace.TypeAndRelationToCheck{
			{
				NamespaceName: req.GetResource().GetObjectType(),
				RelationName:  req.GetPermission(),
				AllowEllipsis: false,
			},
			{
				NamespaceName: req.GetSubject().GetObject().GetObjectType(),
				RelationName:  normalizeSubjectRelation(req.GetSubject()),
				AllowEllipsis: true,
			},
		}, ds); err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	debugOption := computed.NoDebugging

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		_, isDebuggingEnabled := md[string(requestmeta.RequestDebugInformation)]
		if isDebuggingEnabled {
			debugOption = computed.BasicDebuggingEnabled
		}
	}

	if req.GetWithTracing() {
		debugOption = computed.BasicDebuggingEnabled
	}

	cr, metadata, err := computed.ComputeCheck(ctx, ps.dispatch,
		ps.config.CaveatTypeSet,
		computed.CheckParameters{
			ResourceType:  tuple.RR(req.GetResource().GetObjectType(), req.GetPermission()),
			Subject:       tuple.ONR(req.GetSubject().GetObject().GetObjectType(), req.GetSubject().GetObject().GetObjectId(), normalizeSubjectRelation(req.GetSubject())),
			CaveatContext: caveatContext,
			AtRevision:    atRevision,
			MaximumDepth:  ps.config.MaximumAPIDepth,
			DebugOption:   debugOption,
		},
		req.GetResource().GetObjectId(),
		ps.config.DispatchChunkSize,
	)
	usagemetrics.SetInContext(ctx, metadata)

	var debugTrace *v1.DebugInformation
	if debugOption != computed.NoDebugging && metadata.GetDebugInfo() != nil {
		// Convert the dispatch debug information into API debug information.
		converted, cerr := ConvertCheckDispatchDebugInformation(ctx, ps.config.CaveatTypeSet, caveatContext, metadata.GetDebugInfo(), ds)
		if cerr != nil {
			return nil, ps.rewriteError(ctx, cerr)
		}
		debugTrace = converted
	}

	if err != nil {
		// If the error already contains debug information, rewrite it. This can happen if
		// a dispatch error occurs and debug was requested.
		if dispatchDebugInfo, ok := spiceerrors.GetDetails[*dispatch.DebugInformation](err); ok {
			// Convert the dispatch debug information into API debug information.
			converted, cerr := ConvertCheckDispatchDebugInformation(ctx, ps.config.CaveatTypeSet, caveatContext, dispatchDebugInfo, ds)
			if cerr != nil {
				return nil, ps.rewriteError(ctx, cerr)
			}

			if converted != nil {
				return nil, spiceerrors.AppendDetailsMetadata(err, spiceerrors.DebugTraceErrorDetailsKey, converted.String())
			}
		}

		return nil, ps.rewriteErrorWithOptionalDebugTrace(ctx, err, debugTrace)
	}

	permissionship, partialCaveat := checkResultToAPITypes(cr)

	return &v1.CheckPermissionResponse{
		CheckedAt:         checkedAt,
		Permissionship:    permissionship,
		PartialCaveatInfo: partialCaveat,
		DebugTrace:        debugTrace,
	}, nil
}

func checkResultToAPITypes(cr *dispatch.ResourceCheckResult) (v1.CheckPermissionResponse_Permissionship, *v1.PartialCaveatInfo) {
	var partialCaveat *v1.PartialCaveatInfo
	permissionship := v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
	switch cr.GetMembership() {
	case dispatch.ResourceCheckResult_MEMBER:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
	case dispatch.ResourceCheckResult_CAVEATED_MEMBER:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION
		partialCaveat = &v1.PartialCaveatInfo{
			MissingRequiredContext: cr.GetMissingExprFields(),
		}
	}
	return permissionship, partialCaveat
}

func (ps *permissionServer) CheckBulkPermissions(ctx context.Context, req *v1.CheckBulkPermissionsRequest) (*v1.CheckBulkPermissionsResponse, error) {
	// NOTE: perfinsights are added for the individual check results as well, so there is no shape here.
	perfinsights.SetInContext(ctx, perfinsights.NoLabels)

	res, err := ps.bulkChecker.checkBulkPermissions(ctx, req)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	return res, nil
}

func pairItemFromCheckResult(checkResult *dispatch.ResourceCheckResult, debugTrace *v1.DebugInformation) *v1.CheckBulkPermissionsPair_Item {
	permissionship, partialCaveat := checkResultToAPITypes(checkResult)
	return &v1.CheckBulkPermissionsPair_Item{
		Item: &v1.CheckBulkPermissionsResponseItem{
			Permissionship:    permissionship,
			PartialCaveatInfo: partialCaveat,
			DebugTrace:        debugTrace,
		},
	}
}

func requestItemFromResourceAndParameters(params *computed.CheckParameters, resourceID string) (*v1.CheckBulkPermissionsRequestItem, error) {
	item := &v1.CheckBulkPermissionsRequestItem{
		Resource: &v1.ObjectReference{
			ObjectType: params.ResourceType.ObjectType,
			ObjectId:   resourceID,
		},
		Permission: params.ResourceType.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: params.Subject.ObjectType,
				ObjectId:   params.Subject.ObjectID,
			},
			OptionalRelation: denormalizeSubjectRelation(params.Subject.Relation),
		},
	}
	if len(params.CaveatContext) > 0 {
		var err error
		item.Context, err = structpb.NewStruct(params.CaveatContext)
		if err != nil {
			return nil, fmt.Errorf("caveat context wasn't properly validated: %w", err)
		}
	}
	return item, nil
}

func (ps *permissionServer) ExpandPermissionTree(ctx context.Context, req *v1.ExpandPermissionTreeRequest) (*v1.ExpandPermissionTreeResponse, error) {
	perfinsights.SetInContext(ctx, func() perfinsights.APIShapeLabels {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     req.GetResource().GetObjectType(),
			perfinsights.ResourceRelationLabel: req.GetPermission(),
		}
	})

	telemetry.LogicalChecks.Inc()

	atRevision, expandedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	err = namespace.CheckNamespaceAndRelation(ctx, req.GetResource().GetObjectType(), req.GetPermission(), false, ds)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	bf, err := dispatch.NewTraversalBloomFilter(uint(ps.config.MaximumAPIDepth))
	if err != nil {
		return nil, err
	}

	resp, err := ps.dispatch.DispatchExpand(ctx, &dispatch.DispatchExpandRequest{
		Metadata: &dispatch.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: ps.config.MaximumAPIDepth,
			TraversalBloom: bf,
		},
		ResourceAndRelation: &core.ObjectAndRelation{
			Namespace: req.GetResource().GetObjectType(),
			ObjectId:  req.GetResource().GetObjectId(),
			Relation:  req.GetPermission(),
		},
		ExpansionMode: dispatch.DispatchExpandRequest_SHALLOW,
	})
	usagemetrics.SetInContext(ctx, resp.GetMetadata())
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// TODO(jschorr): Change to either using shared interfaces for nodes, or switch the internal
	// dispatched expand to return V1 node types.
	return &v1.ExpandPermissionTreeResponse{
		TreeRoot:   TranslateExpansionTree(resp.GetTreeNode()),
		ExpandedAt: expandedAt,
	}, nil
}

// TranslateRelationshipTree translates a V1 PermissionRelationshipTree into a RelationTupleTreeNode.
func TranslateRelationshipTree(tree *v1.PermissionRelationshipTree) *core.RelationTupleTreeNode {
	var expanded *core.ObjectAndRelation
	if tree.GetExpandedObject() != nil {
		expanded = &core.ObjectAndRelation{
			Namespace: tree.GetExpandedObject().GetObjectType(),
			ObjectId:  tree.GetExpandedObject().GetObjectId(),
			Relation:  tree.GetExpandedRelation(),
		}
	}

	switch t := tree.GetTreeType().(type) {
	case *v1.PermissionRelationshipTree_Intermediate:
		var operation core.SetOperationUserset_Operation
		switch t.Intermediate.GetOperation() {
		case v1.AlgebraicSubjectSet_OPERATION_EXCLUSION:
			operation = core.SetOperationUserset_EXCLUSION
		case v1.AlgebraicSubjectSet_OPERATION_INTERSECTION:
			operation = core.SetOperationUserset_INTERSECTION
		case v1.AlgebraicSubjectSet_OPERATION_UNION:
			operation = core.SetOperationUserset_UNION
		default:
			panic("unknown set operation")
		}

		children := []*core.RelationTupleTreeNode{}
		for _, child := range t.Intermediate.GetChildren() {
			children = append(children, TranslateRelationshipTree(child))
		}

		return &core.RelationTupleTreeNode{
			NodeType: &core.RelationTupleTreeNode_IntermediateNode{
				IntermediateNode: &core.SetOperationUserset{
					Operation:  operation,
					ChildNodes: children,
				},
			},
			Expanded: expanded,
		}

	case *v1.PermissionRelationshipTree_Leaf:
		var subjects []*core.DirectSubject
		for _, subj := range t.Leaf.GetSubjects() {
			subjects = append(subjects, &core.DirectSubject{
				Subject: &core.ObjectAndRelation{
					Namespace: subj.GetObject().GetObjectType(),
					ObjectId:  subj.GetObject().GetObjectId(),
					Relation:  cmp.Or(subj.GetOptionalRelation(), graph.Ellipsis),
				},
			})
		}

		return &core.RelationTupleTreeNode{
			NodeType: &core.RelationTupleTreeNode_LeafNode{
				LeafNode: &core.DirectSubjects{Subjects: subjects},
			},
			Expanded: expanded,
		}

	default:
		panic("unknown type of expansion tree node")
	}
}

func TranslateExpansionTree(node *core.RelationTupleTreeNode) *v1.PermissionRelationshipTree {
	switch t := node.GetNodeType().(type) {
	case *core.RelationTupleTreeNode_IntermediateNode:
		var operation v1.AlgebraicSubjectSet_Operation
		switch t.IntermediateNode.GetOperation() {
		case core.SetOperationUserset_EXCLUSION:
			operation = v1.AlgebraicSubjectSet_OPERATION_EXCLUSION
		case core.SetOperationUserset_INTERSECTION:
			operation = v1.AlgebraicSubjectSet_OPERATION_INTERSECTION
		case core.SetOperationUserset_UNION:
			operation = v1.AlgebraicSubjectSet_OPERATION_UNION
		default:
			panic("unknown set operation")
		}

		var children []*v1.PermissionRelationshipTree
		for _, child := range node.GetIntermediateNode().GetChildNodes() {
			children = append(children, TranslateExpansionTree(child))
		}

		var objRef *v1.ObjectReference
		var objRel string
		if node.GetExpanded() != nil {
			objRef = &v1.ObjectReference{
				ObjectType: node.GetExpanded().GetNamespace(),
				ObjectId:   node.GetExpanded().GetObjectId(),
			}
			objRel = node.GetExpanded().GetRelation()
		}

		return &v1.PermissionRelationshipTree{
			TreeType: &v1.PermissionRelationshipTree_Intermediate{
				Intermediate: &v1.AlgebraicSubjectSet{
					Operation: operation,
					Children:  children,
				},
			},
			ExpandedObject:   objRef,
			ExpandedRelation: objRel,
		}

	case *core.RelationTupleTreeNode_LeafNode:
		var subjects []*v1.SubjectReference
		for _, found := range t.LeafNode.GetSubjects() {
			subjects = append(subjects, &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: found.GetSubject().GetNamespace(),
					ObjectId:   found.GetSubject().GetObjectId(),
				},
				OptionalRelation: denormalizeSubjectRelation(found.GetSubject().GetRelation()),
			})
		}

		if node.GetExpanded() == nil {
			return &v1.PermissionRelationshipTree{
				TreeType: &v1.PermissionRelationshipTree_Leaf{
					Leaf: &v1.DirectSubjectSet{
						Subjects: subjects,
					},
				},
			}
		}

		return &v1.PermissionRelationshipTree{
			TreeType: &v1.PermissionRelationshipTree_Leaf{
				Leaf: &v1.DirectSubjectSet{
					Subjects: subjects,
				},
			},
			ExpandedObject: &v1.ObjectReference{
				ObjectType: node.GetExpanded().GetNamespace(),
				ObjectId:   node.GetExpanded().GetObjectId(),
			},
			ExpandedRelation: node.GetExpanded().GetRelation(),
		}

	default:
		panic("unknown type of expansion tree node")
	}
}

const (
	lrv2CursorFlag = "lrv2"
	lrv3CursorFlag = "lrv3"
)

func (ps *permissionServer) LookupResources(req *v1.LookupResourcesRequest, resp v1.PermissionsService_LookupResourcesServer) error {
	perfinsights.SetInContext(resp.Context(), func() perfinsights.APIShapeLabels {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     req.GetResourceObjectType(),
			perfinsights.ResourceRelationLabel: req.GetPermission(),
			perfinsights.SubjectTypeLabel:      req.GetSubject().GetObject().GetObjectType(),
			perfinsights.SubjectRelationLabel:  req.GetSubject().GetOptionalRelation(),
		}
	})

	if req.GetOptionalCursor() != nil {
		_, isLR3, err := cursor.GetCursorFlag(req.GetOptionalCursor(), lrv3CursorFlag)
		if err != nil {
			return ps.rewriteError(resp.Context(), err)
		}

		if isLR3 {
			return ps.lookupResources3(req, resp)
		}

		_, isLR2, err := cursor.GetCursorFlag(req.GetOptionalCursor(), lrv2CursorFlag)
		if err != nil {
			return ps.rewriteError(resp.Context(), err)
		}

		if isLR2 {
			return ps.lookupResources2(req, resp)
		}
	}

	if ps.config.EnableExperimentalLookupResources3 {
		return ps.lookupResources3(req, resp)
	}

	return ps.lookupResources2(req, resp)
}

func (ps *permissionServer) lookupResources3(req *v1.LookupResourcesRequest, resp v1.PermissionsService_LookupResourcesServer) error {
	if req.GetOptionalLimit() > 0 && req.GetOptionalLimit() > ps.config.MaxLookupResourcesLimit {
		return ps.rewriteError(resp.Context(), NewExceedsMaximumLimitErr(uint64(req.GetOptionalLimit()), uint64(ps.config.MaxLookupResourcesLimit)))
	}

	ctx := resp.Context()

	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := namespace.CheckNamespaceAndRelations(ctx,
		[]namespace.TypeAndRelationToCheck{
			{
				NamespaceName: req.GetResourceObjectType(),
				RelationName:  req.GetPermission(),
				AllowEllipsis: false,
			},
			{
				NamespaceName: req.GetSubject().GetObject().GetObjectType(),
				RelationName:  normalizeSubjectRelation(req.GetSubject()),
				AllowEllipsis: true,
			},
		}, ds); err != nil {
		return ps.rewriteError(ctx, err)
	}

	respMetadata := &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 0,
		DepthRequired:       1,
		DebugInfo:           nil,
	}
	usagemetrics.SetInContext(ctx, respMetadata)

	var currentCursor []string

	lrRequestHash, err := computeLRRequestHash(req)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	if req.GetOptionalCursor() != nil {
		decodedCursor, _, err := cursor.DecodeToDispatchCursor(req.GetOptionalCursor(), lrRequestHash)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}
		currentCursor = decodedCursor.GetSections()
	}

	alreadyPublishedPermissionedResourceIds := map[string]struct{}{}
	var totalCountPublished uint64
	defer func() {
		telemetry.LogicalChecks.Add(float64(totalCountPublished))
	}()

	stream := dispatchpkg.NewHandlingDispatchStream(ctx, func(result *dispatch.DispatchLookupResources3Response) error {
		for _, item := range result.GetItems() {
			var partial *v1.PartialCaveatInfo
			permissionship := v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
			if len(item.GetMissingContextParams()) > 0 {
				permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION
				partial = &v1.PartialCaveatInfo{
					MissingRequiredContext: item.GetMissingContextParams(),
				}
			} else if req.GetOptionalLimit() == 0 {
				if _, ok := alreadyPublishedPermissionedResourceIds[item.GetResourceId()]; ok {
					// Skip publishing the duplicate.
					continue
				}

				// TODO(jschorr): Investigate something like a Trie here for better memory efficiency.
				alreadyPublishedPermissionedResourceIds[item.GetResourceId()] = struct{}{}
			}

			var encodedCursor *v1.Cursor
			if len(item.GetAfterResponseCursorSections()) > 0 {
				currentCursor = item.GetAfterResponseCursorSections()

				ec, err := cursor.EncodeFromDispatchCursorSections(currentCursor, lrRequestHash, atRevision, map[string]string{
					lrv3CursorFlag: "1",
				})
				if err != nil {
					return ps.rewriteError(ctx, err)
				}
				encodedCursor = ec
			}

			err = resp.Send(&v1.LookupResourcesResponse{
				LookedUpAt:        revisionReadAt,
				ResourceObjectId:  item.GetResourceId(),
				Permissionship:    permissionship,
				PartialCaveatInfo: partial,
				AfterResultCursor: encodedCursor,
			})
			if err != nil {
				return err
			}

			totalCountPublished++
		}

		return nil
	})

	bf, err := dispatch.NewTraversalBloomFilter(uint(ps.config.MaximumAPIDepth))
	if err != nil {
		return err
	}

	err = ps.dispatch.DispatchLookupResources3(
		&dispatch.DispatchLookupResources3Request{
			Metadata: &dispatch.ResolverMeta{
				AtRevision:     atRevision.String(),
				DepthRemaining: ps.config.MaximumAPIDepth,
				TraversalBloom: bf,
			},
			ResourceRelation: &core.RelationReference{
				Namespace: req.GetResourceObjectType(),
				Relation:  req.GetPermission(),
			},
			SubjectRelation: &core.RelationReference{
				Namespace: req.GetSubject().GetObject().GetObjectType(),
				Relation:  normalizeSubjectRelation(req.GetSubject()),
			},
			SubjectIds: []string{req.GetSubject().GetObject().GetObjectId()},
			TerminalSubject: &core.ObjectAndRelation{
				Namespace: req.GetSubject().GetObject().GetObjectType(),
				ObjectId:  req.GetSubject().GetObject().GetObjectId(),
				Relation:  normalizeSubjectRelation(req.GetSubject()),
			},
			Context:        req.GetContext(),
			OptionalCursor: currentCursor,
			OptionalLimit:  req.GetOptionalLimit(),
		},
		stream)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	return nil
}

func (ps *permissionServer) lookupResources2(req *v1.LookupResourcesRequest, resp v1.PermissionsService_LookupResourcesServer) error {
	if req.GetOptionalLimit() > 0 && req.GetOptionalLimit() > ps.config.MaxLookupResourcesLimit {
		return ps.rewriteError(resp.Context(), NewExceedsMaximumLimitErr(uint64(req.GetOptionalLimit()), uint64(ps.config.MaxLookupResourcesLimit)))
	}

	ctx := resp.Context()

	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := namespace.CheckNamespaceAndRelations(ctx,
		[]namespace.TypeAndRelationToCheck{
			{
				NamespaceName: req.GetResourceObjectType(),
				RelationName:  req.GetPermission(),
				AllowEllipsis: false,
			},
			{
				NamespaceName: req.GetSubject().GetObject().GetObjectType(),
				RelationName:  normalizeSubjectRelation(req.GetSubject()),
				AllowEllipsis: true,
			},
		}, ds); err != nil {
		return ps.rewriteError(ctx, err)
	}

	respMetadata := &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 0,
		DepthRequired:       1,
		DebugInfo:           nil,
	}
	usagemetrics.SetInContext(ctx, respMetadata)

	var currentCursor *dispatch.Cursor

	lrRequestHash, err := computeLRRequestHash(req)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	if req.GetOptionalCursor() != nil {
		decodedCursor, _, err := cursor.DecodeToDispatchCursor(req.GetOptionalCursor(), lrRequestHash)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}
		currentCursor = decodedCursor
	}

	alreadyPublishedPermissionedResourceIds := map[string]struct{}{}
	var totalCountPublished uint64
	defer func() {
		telemetry.LogicalChecks.Add(float64(totalCountPublished))
	}()

	stream := dispatchpkg.NewHandlingDispatchStream(ctx, func(result *dispatch.DispatchLookupResources2Response) error {
		found := result.GetResource()

		dispatchpkg.AddResponseMetadata(respMetadata, result.GetMetadata())
		currentCursor = result.GetAfterResponseCursor()

		var partial *v1.PartialCaveatInfo
		permissionship := v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
		if len(found.GetMissingContextParams()) > 0 {
			permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION
			partial = &v1.PartialCaveatInfo{
				MissingRequiredContext: found.GetMissingContextParams(),
			}
		} else if req.GetOptionalLimit() == 0 {
			if _, ok := alreadyPublishedPermissionedResourceIds[found.GetResourceId()]; ok {
				// Skip publishing the duplicate.
				return nil
			}

			// TODO(jschorr): Investigate something like a Trie here for better memory efficiency.
			alreadyPublishedPermissionedResourceIds[found.GetResourceId()] = struct{}{}
		}

		encodedCursor, err := cursor.EncodeFromDispatchCursor(result.GetAfterResponseCursor(), lrRequestHash, atRevision, map[string]string{
			lrv2CursorFlag: "1",
		})
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		err = resp.Send(&v1.LookupResourcesResponse{
			LookedUpAt:        revisionReadAt,
			ResourceObjectId:  found.GetResourceId(),
			Permissionship:    permissionship,
			PartialCaveatInfo: partial,
			AfterResultCursor: encodedCursor,
		})
		if err != nil {
			return err
		}

		totalCountPublished++
		return nil
	})

	bf, err := dispatch.NewTraversalBloomFilter(uint(ps.config.MaximumAPIDepth))
	if err != nil {
		return err
	}

	err = ps.dispatch.DispatchLookupResources2(
		&dispatch.DispatchLookupResources2Request{
			Metadata: &dispatch.ResolverMeta{
				AtRevision:     atRevision.String(),
				DepthRemaining: ps.config.MaximumAPIDepth,
				TraversalBloom: bf,
			},
			ResourceRelation: &core.RelationReference{
				Namespace: req.GetResourceObjectType(),
				Relation:  req.GetPermission(),
			},
			SubjectRelation: &core.RelationReference{
				Namespace: req.GetSubject().GetObject().GetObjectType(),
				Relation:  normalizeSubjectRelation(req.GetSubject()),
			},
			SubjectIds: []string{req.GetSubject().GetObject().GetObjectId()},
			TerminalSubject: &core.ObjectAndRelation{
				Namespace: req.GetSubject().GetObject().GetObjectType(),
				ObjectId:  req.GetSubject().GetObject().GetObjectId(),
				Relation:  normalizeSubjectRelation(req.GetSubject()),
			},
			Context:        req.GetContext(),
			OptionalCursor: currentCursor,
			OptionalLimit:  req.GetOptionalLimit(),
		},
		stream)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	return nil
}

func (ps *permissionServer) LookupSubjects(req *v1.LookupSubjectsRequest, resp v1.PermissionsService_LookupSubjectsServer) error {
	perfinsights.SetInContext(resp.Context(), func() perfinsights.APIShapeLabels {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     req.GetResource().GetObjectType(),
			perfinsights.ResourceRelationLabel: req.GetPermission(),
			perfinsights.SubjectTypeLabel:      req.GetSubjectObjectType(),
			perfinsights.SubjectRelationLabel:  req.GetOptionalSubjectRelation(),
		}
	})

	ctx := resp.Context()

	if req.GetOptionalConcreteLimit() != 0 {
		return ps.rewriteError(ctx, status.Errorf(codes.Unimplemented, "concrete limit is not yet supported"))
	}

	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	caveatContext, err := GetCaveatContext(ctx, req.GetContext(), ps.config.MaxCaveatContextSize)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	if err := namespace.CheckNamespaceAndRelations(ctx,
		[]namespace.TypeAndRelationToCheck{
			{
				NamespaceName: req.GetResource().GetObjectType(),
				RelationName:  req.GetPermission(),
				AllowEllipsis: false,
			},
			{
				NamespaceName: req.GetSubjectObjectType(),
				RelationName:  cmp.Or(req.GetOptionalSubjectRelation(), tuple.Ellipsis),
				AllowEllipsis: true,
			},
		}, ds); err != nil {
		return ps.rewriteError(ctx, err)
	}

	respMetadata := &dispatch.ResponseMeta{
		DispatchCount:       0,
		CachedDispatchCount: 0,
		DepthRequired:       0,
		DebugInfo:           nil,
	}
	usagemetrics.SetInContext(ctx, respMetadata)

	var totalCountPublished uint64
	defer func() {
		telemetry.LogicalChecks.Add(float64(totalCountPublished))
	}()

	stream := dispatchpkg.NewHandlingDispatchStream(ctx, func(result *dispatch.DispatchLookupSubjectsResponse) error {
		foundSubjects, ok := result.GetFoundSubjectsByResourceId()[req.GetResource().GetObjectId()]
		if !ok {
			return errors.New("missing resource ID in returned LS")
		}

		for _, foundSubject := range foundSubjects.GetFoundSubjects() {
			excludedSubjectIDs := make([]string, 0, len(foundSubject.GetExcludedSubjects()))
			for _, excludedSubject := range foundSubject.GetExcludedSubjects() {
				excludedSubjectIDs = append(excludedSubjectIDs, excludedSubject.GetSubjectId())
			}

			excludedSubjects := make([]*v1.ResolvedSubject, 0, len(foundSubject.GetExcludedSubjects()))
			for _, excludedSubject := range foundSubject.GetExcludedSubjects() {
				resolvedExcludedSubject, err := foundSubjectToResolvedSubject(ctx, excludedSubject, caveatContext, ds, ps.config.CaveatTypeSet)
				if err != nil {
					return err
				}

				if resolvedExcludedSubject == nil {
					continue
				}

				excludedSubjects = append(excludedSubjects, resolvedExcludedSubject)
			}

			subject, err := foundSubjectToResolvedSubject(ctx, foundSubject, caveatContext, ds, ps.config.CaveatTypeSet)
			if err != nil {
				return err
			}
			if subject == nil {
				continue
			}

			err = resp.Send(&v1.LookupSubjectsResponse{
				Subject:            subject,
				ExcludedSubjects:   excludedSubjects,
				LookedUpAt:         revisionReadAt,
				SubjectObjectId:    foundSubject.GetSubjectId(),    // Deprecated
				ExcludedSubjectIds: excludedSubjectIDs,             // Deprecated
				Permissionship:     subject.GetPermissionship(),    // Deprecated
				PartialCaveatInfo:  subject.GetPartialCaveatInfo(), // Deprecated
			})
			if err != nil {
				return err
			}
		}

		totalCountPublished++
		dispatchpkg.AddResponseMetadata(respMetadata, result.GetMetadata())
		return nil
	})

	bf, err := dispatch.NewTraversalBloomFilter(uint(ps.config.MaximumAPIDepth))
	if err != nil {
		return err
	}

	err = ps.dispatch.DispatchLookupSubjects(
		&dispatch.DispatchLookupSubjectsRequest{
			Metadata: &dispatch.ResolverMeta{
				AtRevision:     atRevision.String(),
				DepthRemaining: ps.config.MaximumAPIDepth,
				TraversalBloom: bf,
			},
			ResourceRelation: &core.RelationReference{
				Namespace: req.GetResource().GetObjectType(),
				Relation:  req.GetPermission(),
			},
			ResourceIds: []string{req.GetResource().GetObjectId()},
			SubjectRelation: &core.RelationReference{
				Namespace: req.GetSubjectObjectType(),
				Relation:  cmp.Or(req.GetOptionalSubjectRelation(), tuple.Ellipsis),
			},
		},
		stream)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	return nil
}

func foundSubjectToResolvedSubject(ctx context.Context, foundSubject *dispatch.FoundSubject, caveatContext map[string]any, ds datastore.CaveatReader, caveatTypeSet *caveattypes.TypeSet) (*v1.ResolvedSubject, error) {
	var partialCaveat *v1.PartialCaveatInfo
	permissionship := v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
	if foundSubject.GetCaveatExpression() != nil {
		permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION

		cr, err := cexpr.RunSingleCaveatExpression(ctx, caveatTypeSet, foundSubject.GetCaveatExpression(), caveatContext, ds, cexpr.RunCaveatExpressionNoDebugging)
		if err != nil {
			return nil, err
		}

		switch {
		case cr.Value():
			permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
		case cr.IsPartial():
			missingFields, _ := cr.MissingVarNames()
			partialCaveat = &v1.PartialCaveatInfo{
				MissingRequiredContext: missingFields,
			}
		default:
			// Skip this found subject.
			return nil, nil
		}
	}

	return &v1.ResolvedSubject{
		SubjectObjectId:   foundSubject.GetSubjectId(),
		Permissionship:    permissionship,
		PartialCaveatInfo: partialCaveat,
	}, nil
}

func normalizeSubjectRelation(sub *v1.SubjectReference) string {
	if sub.GetOptionalRelation() == "" {
		return graph.Ellipsis
	}
	return sub.GetOptionalRelation()
}

func denormalizeSubjectRelation(relation string) string {
	if relation == graph.Ellipsis {
		return ""
	}
	return relation
}

func GetCaveatContext(ctx context.Context, caveatCtx *structpb.Struct, maxCaveatContextSize int) (map[string]any, error) {
	var caveatContext map[string]any
	if caveatCtx != nil {
		if size := proto.Size(caveatCtx); maxCaveatContextSize > 0 && size > maxCaveatContextSize {
			return nil, shared.RewriteError(
				ctx,
				status.Errorf(
					codes.InvalidArgument,
					"request caveat context should have less than %d bytes but had %d",
					maxCaveatContextSize,
					size,
				),
				nil,
			)
		}
		caveatContext = caveatCtx.AsMap()
	}
	return caveatContext, nil
}

type loadBulkAdapter struct {
	stream                 grpc.ClientStreamingServer[v1.ImportBulkRelationshipsRequest, v1.ImportBulkRelationshipsResponse]
	referencedNamespaceMap map[string]*schema.Definition
	referencedCaveatMap    map[string]*core.CaveatDefinition
	current                tuple.Relationship
	caveat                 core.ContextualizedCaveat
	caveatTypeSet          *caveattypes.TypeSet

	awaitingNamespaces []string
	awaitingCaveats    []string

	currentBatch []*v1.Relationship
	numSent      int
	err          error
}

func (a *loadBulkAdapter) Next(_ context.Context) (*tuple.Relationship, error) {
	for a.err == nil && a.numSent == len(a.currentBatch) {
		// Load a new batch
		batch, err := a.stream.Recv()
		if err != nil {
			a.err = err
			if errors.Is(a.err, io.EOF) {
				return nil, nil
			}
			return nil, a.err
		}

		a.currentBatch = batch.GetRelationships()
		a.numSent = 0

		a.awaitingNamespaces, a.awaitingCaveats = extractBatchNewReferencedNamespacesAndCaveats(
			a.currentBatch,
			a.referencedNamespaceMap,
			a.referencedCaveatMap,
		)
	}

	if len(a.awaitingNamespaces) > 0 || len(a.awaitingCaveats) > 0 {
		// Shut down the stream to give our caller a chance to fill in this information
		return nil, nil
	}

	a.current.Resource.ObjectType = a.currentBatch[a.numSent].GetResource().GetObjectType()
	a.current.Resource.ObjectID = a.currentBatch[a.numSent].GetResource().GetObjectId()
	a.current.Resource.Relation = a.currentBatch[a.numSent].GetRelation()
	a.current.Subject.ObjectType = a.currentBatch[a.numSent].GetSubject().GetObject().GetObjectType()
	a.current.Subject.ObjectID = a.currentBatch[a.numSent].GetSubject().GetObject().GetObjectId()
	a.current.Subject.Relation = cmp.Or(a.currentBatch[a.numSent].GetSubject().GetOptionalRelation(), tuple.Ellipsis)

	if a.currentBatch[a.numSent].GetOptionalCaveat() != nil {
		a.caveat.CaveatName = a.currentBatch[a.numSent].GetOptionalCaveat().GetCaveatName()
		a.caveat.Context = a.currentBatch[a.numSent].GetOptionalCaveat().GetContext()
		a.current.OptionalCaveat = &a.caveat
	} else {
		a.current.OptionalCaveat = nil
	}

	if a.currentBatch[a.numSent].GetOptionalExpiresAt() != nil {
		t := a.currentBatch[a.numSent].GetOptionalExpiresAt().AsTime()
		a.current.OptionalExpiration = &t
	} else {
		a.current.OptionalExpiration = nil
	}

	a.current.OptionalIntegrity = nil

	if err := relationships.ValidateOneRelationship(
		a.referencedNamespaceMap,
		a.referencedCaveatMap,
		a.caveatTypeSet,
		a.current,
		relationships.ValidateRelationshipForCreateOrTouch,
	); err != nil {
		return nil, err
	}

	a.numSent++
	return &a.current, nil
}

func (ps *permissionServer) ImportBulkRelationships(stream grpc.ClientStreamingServer[v1.ImportBulkRelationshipsRequest, v1.ImportBulkRelationshipsResponse]) error {
	perfinsights.SetInContext(stream.Context(), perfinsights.NoLabels)

	ds := datastoremw.MustFromContext(stream.Context())

	var numWritten uint64
	if _, err := ds.ReadWriteTx(stream.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loadedNamespaces := make(map[string]*schema.Definition, 2)
		loadedCaveats := make(map[string]*core.CaveatDefinition, 0)

		adapter := &loadBulkAdapter{
			stream:                 stream,
			referencedNamespaceMap: loadedNamespaces,
			referencedCaveatMap:    loadedCaveats,
			caveat:                 core.ContextualizedCaveat{},
			caveatTypeSet:          ps.config.CaveatTypeSet,
		}
		resolver := schema.ResolverForDatastoreReader(rwt)
		ts := schema.NewTypeSystem(resolver)

		var streamWritten uint64
		var err error
		for ; adapter.err == nil && err == nil; streamWritten, err = rwt.BulkLoad(stream.Context(), adapter) {
			numWritten += streamWritten

			// The stream has terminated because we're awaiting namespace and/or caveat information
			if len(adapter.awaitingNamespaces) > 0 {
				nsDefs, err := rwt.LookupNamespacesWithNames(stream.Context(), adapter.awaitingNamespaces)
				if err != nil {
					return err
				}

				for _, nsDef := range nsDefs {
					newDef, err := schema.NewDefinition(ts, nsDef.Definition)
					if err != nil {
						return err
					}

					loadedNamespaces[nsDef.Definition.GetName()] = newDef
				}
				adapter.awaitingNamespaces = nil
			}

			if len(adapter.awaitingCaveats) > 0 {
				caveats, err := rwt.LookupCaveatsWithNames(stream.Context(), adapter.awaitingCaveats)
				if err != nil {
					return err
				}

				for _, caveat := range caveats {
					loadedCaveats[caveat.Definition.GetName()] = caveat.Definition
				}
				adapter.awaitingCaveats = nil
			}
		}
		numWritten += streamWritten

		return err
	}, dsoptions.WithDisableRetries(true)); err != nil {
		return shared.RewriteErrorWithoutConfig(stream.Context(), err)
	}

	usagemetrics.SetInContext(stream.Context(), &dispatch.ResponseMeta{
		// One request for the whole load
		DispatchCount: 1,
	})

	return stream.SendAndClose(&v1.ImportBulkRelationshipsResponse{
		NumLoaded: numWritten,
	})
}

func (ps *permissionServer) ExportBulkRelationships(
	req *v1.ExportBulkRelationshipsRequest,
	resp grpc.ServerStreamingServer[v1.ExportBulkRelationshipsResponse],
) error {
	ctx := resp.Context()
	perfinsights.SetInContext(ctx, func() perfinsights.APIShapeLabels {
		return labelsForFilter(req.GetOptionalRelationshipFilter())
	})

	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return ExportBulk(ctx, datastoremw.MustFromContext(ctx), uint64(ps.config.MaxBulkExportRelationshipsLimit), req, atRevision, resp.Send)
}

// ExportBulk implements the ExportBulkRelationships API functionality. Given a datastore.Datastore, it will
// export stream via the sender all relationships matched by the incoming request.
// If no cursor is provided, it will fallback to the provided revision.
func ExportBulk(ctx context.Context, ds datastore.Datastore, batchSize uint64, req *v1.ExportBulkRelationshipsRequest, fallbackRevision datastore.Revision, sender func(response *v1.ExportBulkRelationshipsResponse) error) error {
	if req.GetOptionalLimit() > 0 && uint64(req.GetOptionalLimit()) > batchSize {
		return shared.RewriteErrorWithoutConfig(ctx, NewExceedsMaximumLimitErr(uint64(req.GetOptionalLimit()), batchSize))
	}

	atRevision := fallbackRevision
	var curNamespace string
	var cur dsoptions.Cursor
	if req.GetOptionalCursor() != nil {
		var err error
		atRevision, curNamespace, cur, err = decodeCursor(ds, req.GetOptionalCursor())
		if err != nil {
			return shared.RewriteErrorWithoutConfig(ctx, err)
		}
	}

	reader := ds.SnapshotReader(atRevision)

	namespaces, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return shared.RewriteErrorWithoutConfig(ctx, err)
	}

	// Make sure the namespaces are always in a stable order
	slices.SortFunc(namespaces, func(
		lhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
		rhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
	) int {
		return strings.Compare(lhs.Definition.GetName(), rhs.Definition.GetName())
	})

	// Skip the namespaces that are already fully returned
	for cur != nil && len(namespaces) > 0 && namespaces[0].Definition.GetName() < curNamespace {
		namespaces = namespaces[1:]
	}

	limit := batchSize
	if req.GetOptionalLimit() > 0 {
		limit = uint64(req.GetOptionalLimit())
	}

	// Pre-allocate all of the relationships that we might need in order to
	// make export easier and faster for the garbage collector.
	relsArray := make([]v1.Relationship, limit)
	objArray := make([]v1.ObjectReference, limit)
	subArray := make([]v1.SubjectReference, limit)
	subObjArray := make([]v1.ObjectReference, limit)
	caveatArray := make([]v1.ContextualizedCaveat, limit)
	for i := range relsArray {
		relsArray[i].Resource = &objArray[i]
		relsArray[i].Subject = &subArray[i]
		relsArray[i].Subject.Object = &subObjArray[i]
	}

	emptyRels := make([]*v1.Relationship, limit)
	// The number of batches/dispatches for the purpose of usage metrics
	var batches uint32
	for _, ns := range namespaces {
		rels := emptyRels

		// Reset the cursor between namespaces.
		if ns.Definition.GetName() != curNamespace {
			cur = nil
		}

		// Skip this namespace if a resource type filter was specified.
		if req.GetOptionalRelationshipFilter() != nil && req.GetOptionalRelationshipFilter().GetResourceType() != "" {
			if ns.Definition.GetName() != req.GetOptionalRelationshipFilter().GetResourceType() {
				continue
			}
		}

		// Setup the filter to use for the relationships.
		relationshipFilter := datastore.RelationshipsFilter{OptionalResourceType: ns.Definition.GetName()}
		if req.GetOptionalRelationshipFilter() != nil {
			rf, err := datastore.RelationshipsFilterFromPublicFilter(req.GetOptionalRelationshipFilter())
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			// Overload the namespace name with the one from the request, because each iteration is for a different namespace.
			rf.OptionalResourceType = ns.Definition.GetName()
			relationshipFilter = rf
		}

		// We want to keep iterating as long as we're sending full batches.
		// To bootstrap this loop, we enter the first time with a full rels
		// slice of dummy rels that were never sent.
		for uint64(len(rels)) == limit {
			// Lop off any rels we've already sent
			rels = rels[:0]

			relFn := func(rel tuple.Relationship) {
				offset := len(rels)
				rels = append(rels, &relsArray[offset]) // nozero

				v1Rel := &relsArray[offset]
				v1Rel.Resource.ObjectType = rel.Resource.ObjectType
				v1Rel.Resource.ObjectId = rel.Resource.ObjectID
				v1Rel.Relation = rel.Resource.Relation
				v1Rel.Subject.Object.ObjectType = rel.Subject.ObjectType
				v1Rel.Subject.Object.ObjectId = rel.Subject.ObjectID
				v1Rel.Subject.OptionalRelation = denormalizeSubjectRelation(rel.Subject.Relation)

				if rel.OptionalCaveat != nil {
					caveatArray[offset].CaveatName = rel.OptionalCaveat.GetCaveatName()
					caveatArray[offset].Context = rel.OptionalCaveat.GetContext()
					v1Rel.OptionalCaveat = &caveatArray[offset]
				} else {
					caveatArray[offset].CaveatName = ""
					caveatArray[offset].Context = nil
					v1Rel.OptionalCaveat = nil
				}

				if rel.OptionalExpiration != nil {
					v1Rel.OptionalExpiresAt = timestamppb.New(*rel.OptionalExpiration)
				} else {
					v1Rel.OptionalExpiresAt = nil
				}
			}

			cur, err = queryForEach(
				ctx,
				reader,
				relationshipFilter,
				relFn,
				dsoptions.WithLimit(&limit),
				dsoptions.WithAfter(cur),
				dsoptions.WithSort(dsoptions.ByResource),
				dsoptions.WithQueryShape(queryshape.Varying),
			)
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if len(rels) == 0 {
				continue
			}

			encoded, err := cursor.Encode(&implv1.DecodedCursor{
				VersionOneof: &implv1.DecodedCursor_V1{
					V1: &implv1.V1Cursor{
						Revision: atRevision.String(),
						Sections: []string{
							ns.Definition.GetName(),
							tuple.MustString(*dsoptions.ToRelationship(cur)),
						},
					},
				},
			})
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if err := sender(&v1.ExportBulkRelationshipsResponse{
				AfterResultCursor: encoded,
				Relationships:     rels,
			}); err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}
			// Increment batches for usagemetrics
			batches++
		}
	}

	// Record usage metrics
	respMetadata := &dispatch.ResponseMeta{
		DispatchCount: batches,
	}
	usagemetrics.SetInContext(ctx, respMetadata)

	return nil
}
