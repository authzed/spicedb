package v1

import (
	"context"
	"fmt"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	dispatchpkg "github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func (ps *permissionServer) rewriteError(ctx context.Context, err error) error {
	return shared.RewriteError(ctx, err, &shared.ConfigForErrors{
		MaximumAPIDepth: ps.config.MaximumAPIDepth,
	})
}

func (ps *permissionServer) CheckPermission(ctx context.Context, req *v1.CheckPermissionRequest) (*v1.CheckPermissionResponse, error) {
	atRevision, checkedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	if err := namespace.CheckNamespaceAndRelations(ctx,
		[]namespace.TypeAndRelationToCheck{
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

	cr, metadata, err := computed.ComputeCheck(ctx, ps.dispatch,
		computed.CheckParameters{
			ResourceType: &core.RelationReference{
				Namespace: req.Resource.ObjectType,
				Relation:  req.Permission,
			},
			Subject: &core.ObjectAndRelation{
				Namespace: req.Subject.Object.ObjectType,
				ObjectId:  req.Subject.Object.ObjectId,
				Relation:  normalizeSubjectRelation(req.Subject),
			},
			CaveatContext: caveatContext,
			AtRevision:    atRevision,
			MaximumDepth:  ps.config.MaximumAPIDepth,
			DebugOption:   debugOption,
		},
		req.Resource.ObjectId,
	)
	usagemetrics.SetInContext(ctx, metadata)

	if debugOption != computed.NoDebugging && metadata.DebugInfo != nil {
		// Convert the dispatch debug information into API debug information and marshal into
		// the footer.
		converted, cerr := ConvertCheckDispatchDebugInformation(ctx, caveatContext, metadata, ds)
		if cerr != nil {
			return nil, ps.rewriteError(ctx, cerr)
		}

		marshaled, merr := protojson.Marshal(converted)
		if merr != nil {
			return nil, ps.rewriteError(ctx, merr)
		}

		serr := responsemeta.SetResponseTrailerMetadata(ctx, map[responsemeta.ResponseMetadataTrailerKey]string{
			responsemeta.DebugInformation: string(marshaled),
		})
		if serr != nil {
			return nil, ps.rewriteError(ctx, serr)
		}
	}

	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	permissionship, partialCaveat := checkResultToAPITypes(cr)

	return &v1.CheckPermissionResponse{
		CheckedAt:         checkedAt,
		Permissionship:    permissionship,
		PartialCaveatInfo: partialCaveat,
	}, nil
}

func checkResultToAPITypes(cr *dispatch.ResourceCheckResult) (v1.CheckPermissionResponse_Permissionship, *v1.PartialCaveatInfo) {
	var partialCaveat *v1.PartialCaveatInfo
	permissionship := v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
	if cr.Membership == dispatch.ResourceCheckResult_MEMBER {
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
	} else if cr.Membership == dispatch.ResourceCheckResult_CAVEATED_MEMBER {
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION
		partialCaveat = &v1.PartialCaveatInfo{
			MissingRequiredContext: cr.MissingExprFields,
		}
	}
	return permissionship, partialCaveat
}

func (ps *permissionServer) ExpandPermissionTree(ctx context.Context, req *v1.ExpandPermissionTreeRequest) (*v1.ExpandPermissionTreeResponse, error) {
	atRevision, expandedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	err = namespace.CheckNamespaceAndRelation(ctx, req.Resource.ObjectType, req.Permission, false, ds)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	resp, err := ps.dispatch.DispatchExpand(ctx, &dispatch.DispatchExpandRequest{
		Metadata: &dispatch.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: ps.config.MaximumAPIDepth,
		},
		ResourceAndRelation: &core.ObjectAndRelation{
			Namespace: req.Resource.ObjectType,
			ObjectId:  req.Resource.ObjectId,
			Relation:  req.Permission,
		},
		ExpansionMode: dispatch.DispatchExpandRequest_SHALLOW,
	})
	usagemetrics.SetInContext(ctx, resp.Metadata)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	// TODO(jschorr): Change to either using shared interfaces for nodes, or switch the internal
	// dispatched expand to return V1 node types.
	return &v1.ExpandPermissionTreeResponse{
		TreeRoot:   TranslateExpansionTree(resp.TreeNode),
		ExpandedAt: expandedAt,
	}, nil
}

// TranslateRelationshipTree translates a V1 PermissionRelationshipTree into a RelationTupleTreeNode.
func TranslateRelationshipTree(tree *v1.PermissionRelationshipTree) *core.RelationTupleTreeNode {
	var expanded *core.ObjectAndRelation
	if tree.ExpandedObject != nil {
		expanded = &core.ObjectAndRelation{
			Namespace: tree.ExpandedObject.ObjectType,
			ObjectId:  tree.ExpandedObject.ObjectId,
			Relation:  tree.ExpandedRelation,
		}
	}

	switch t := tree.TreeType.(type) {
	case *v1.PermissionRelationshipTree_Intermediate:
		var operation core.SetOperationUserset_Operation
		switch t.Intermediate.Operation {
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
		for _, child := range t.Intermediate.Children {
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
		for _, subj := range t.Leaf.Subjects {
			subjects = append(subjects, &core.DirectSubject{
				Subject: &core.ObjectAndRelation{
					Namespace: subj.Object.ObjectType,
					ObjectId:  subj.Object.ObjectId,
					Relation:  stringz.DefaultEmpty(subj.OptionalRelation, graph.Ellipsis),
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
	switch t := node.NodeType.(type) {
	case *core.RelationTupleTreeNode_IntermediateNode:
		var operation v1.AlgebraicSubjectSet_Operation
		switch t.IntermediateNode.Operation {
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
		for _, child := range node.GetIntermediateNode().ChildNodes {
			children = append(children, TranslateExpansionTree(child))
		}

		var objRef *v1.ObjectReference
		var objRel string
		if node.Expanded != nil {
			objRef = &v1.ObjectReference{
				ObjectType: node.Expanded.Namespace,
				ObjectId:   node.Expanded.ObjectId,
			}
			objRel = node.Expanded.Relation
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
		for _, found := range t.LeafNode.Subjects {
			subjects = append(subjects, &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: found.Subject.Namespace,
					ObjectId:   found.Subject.ObjectId,
				},
				OptionalRelation: denormalizeSubjectRelation(found.Subject.Relation),
			})
		}

		if node.Expanded == nil {
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
				ObjectType: node.Expanded.Namespace,
				ObjectId:   node.Expanded.ObjectId,
			},
			ExpandedRelation: node.Expanded.Relation,
		}

	default:
		panic("unknown type of expansion tree node")
	}
}

func (ps *permissionServer) LookupResources(req *v1.LookupResourcesRequest, resp v1.PermissionsService_LookupResourcesServer) error {
	ctx := resp.Context()
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := namespace.CheckNamespaceAndRelations(ctx,
		[]namespace.TypeAndRelationToCheck{
			{
				NamespaceName: req.ResourceObjectType,
				RelationName:  req.Permission,
				AllowEllipsis: false,
			},
			{
				NamespaceName: req.Subject.Object.ObjectType,
				RelationName:  normalizeSubjectRelation(req.Subject),
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

	if req.OptionalCursor != nil {
		decodedCursor, err := cursor.DecodeToDispatchCursor(req.OptionalCursor, lrRequestHash)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}
		currentCursor = decodedCursor
	}

	alreadyPublishedPermissionedResourceIds := map[string]struct{}{}

	stream := dispatchpkg.NewHandlingDispatchStream(ctx, func(result *dispatch.DispatchLookupResourcesResponse) error {
		found := result.ResolvedResource

		dispatchpkg.AddResponseMetadata(respMetadata, result.Metadata)
		currentCursor = result.AfterResponseCursor

		var partial *v1.PartialCaveatInfo
		permissionship := v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
		if found.Permissionship == dispatch.ResolvedResource_CONDITIONALLY_HAS_PERMISSION {
			permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION
			partial = &v1.PartialCaveatInfo{
				MissingRequiredContext: found.MissingRequiredContext,
			}
		} else if req.OptionalLimit == 0 {
			if _, ok := alreadyPublishedPermissionedResourceIds[found.ResourceId]; ok {
				// Skip publishing the duplicate.
				return nil
			}

			alreadyPublishedPermissionedResourceIds[found.ResourceId] = struct{}{}
		}

		encodedCursor, err := cursor.EncodeFromDispatchCursor(result.AfterResponseCursor, lrRequestHash, atRevision)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		err = resp.Send(&v1.LookupResourcesResponse{
			LookedUpAt:        revisionReadAt,
			ResourceObjectId:  found.ResourceId,
			Permissionship:    permissionship,
			PartialCaveatInfo: partial,
			AfterResultCursor: encodedCursor,
		})
		if err != nil {
			return err
		}
		return nil
	})

	err = ps.dispatch.DispatchLookupResources(
		&dispatch.DispatchLookupResourcesRequest{
			Metadata: &dispatch.ResolverMeta{
				AtRevision:     atRevision.String(),
				DepthRemaining: ps.config.MaximumAPIDepth,
			},
			ObjectRelation: &core.RelationReference{
				Namespace: req.ResourceObjectType,
				Relation:  req.Permission,
			},
			Subject: &core.ObjectAndRelation{
				Namespace: req.Subject.Object.ObjectType,
				ObjectId:  req.Subject.Object.ObjectId,
				Relation:  normalizeSubjectRelation(req.Subject),
			},
			Context:        req.Context,
			OptionalCursor: currentCursor,
			OptionalLimit:  req.OptionalLimit,
		},
		stream)

	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	return nil
}

func (ps *permissionServer) LookupSubjects(req *v1.LookupSubjectsRequest, resp v1.PermissionsService_LookupSubjectsServer) error {
	ctx := resp.Context()
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	caveatContext, err := GetCaveatContext(ctx, req.Context, ps.config.MaxCaveatContextSize)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	if err := namespace.CheckNamespaceAndRelations(ctx,
		[]namespace.TypeAndRelationToCheck{
			{
				NamespaceName: req.Resource.ObjectType,
				RelationName:  req.Permission,
				AllowEllipsis: false,
			},
			{
				NamespaceName: req.SubjectObjectType,
				RelationName:  stringz.DefaultEmpty(req.OptionalSubjectRelation, tuple.Ellipsis),
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

	stream := dispatchpkg.NewHandlingDispatchStream(ctx, func(result *dispatch.DispatchLookupSubjectsResponse) error {
		foundSubjects, ok := result.FoundSubjectsByResourceId[req.Resource.ObjectId]
		if !ok {
			return fmt.Errorf("missing resource ID in returned LS")
		}

		for _, foundSubject := range foundSubjects.FoundSubjects {
			excludedSubjectIDs := make([]string, 0, len(foundSubject.ExcludedSubjects))
			for _, excludedSubject := range foundSubject.ExcludedSubjects {
				excludedSubjectIDs = append(excludedSubjectIDs, excludedSubject.SubjectId)
			}

			excludedSubjects := make([]*v1.ResolvedSubject, 0, len(foundSubject.ExcludedSubjects))
			for _, excludedSubject := range foundSubject.ExcludedSubjects {
				resolvedExcludedSubject, err := foundSubjectToResolvedSubject(ctx, excludedSubject, caveatContext, ds)
				if err != nil {
					return err
				}

				if resolvedExcludedSubject == nil {
					continue
				}

				excludedSubjects = append(excludedSubjects, resolvedExcludedSubject)
			}

			subject, err := foundSubjectToResolvedSubject(ctx, foundSubject, caveatContext, ds)
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
				SubjectObjectId:    foundSubject.SubjectId,    // Deprecated
				ExcludedSubjectIds: excludedSubjectIDs,        // Deprecated
				Permissionship:     subject.Permissionship,    // Deprecated
				PartialCaveatInfo:  subject.PartialCaveatInfo, // Deprecated
			})
			if err != nil {
				return err
			}
		}

		dispatchpkg.AddResponseMetadata(respMetadata, result.Metadata)
		return nil
	})

	err = ps.dispatch.DispatchLookupSubjects(
		&dispatch.DispatchLookupSubjectsRequest{
			Metadata: &dispatch.ResolverMeta{
				AtRevision:     atRevision.String(),
				DepthRemaining: ps.config.MaximumAPIDepth,
			},
			ResourceRelation: &core.RelationReference{
				Namespace: req.Resource.ObjectType,
				Relation:  req.Permission,
			},
			ResourceIds: []string{req.Resource.ObjectId},
			SubjectRelation: &core.RelationReference{
				Namespace: req.SubjectObjectType,
				Relation:  stringz.DefaultEmpty(req.OptionalSubjectRelation, tuple.Ellipsis),
			},
		},
		stream)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	return nil
}

func foundSubjectToResolvedSubject(ctx context.Context, foundSubject *dispatch.FoundSubject, caveatContext map[string]any, ds datastore.CaveatReader) (*v1.ResolvedSubject, error) {
	var partialCaveat *v1.PartialCaveatInfo
	permissionship := v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
	if foundSubject.GetCaveatExpression() != nil {
		permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION

		cr, err := cexpr.RunCaveatExpression(ctx, foundSubject.GetCaveatExpression(), caveatContext, ds, cexpr.RunCaveatExpressionNoDebugging)
		if err != nil {
			return nil, err
		}

		if cr.Value() {
			permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
		} else if cr.IsPartial() {
			missingFields, _ := cr.MissingVarNames()
			partialCaveat = &v1.PartialCaveatInfo{
				MissingRequiredContext: missingFields,
			}
		} else {
			// Skip this found subject.
			return nil, nil
		}
	}

	return &v1.ResolvedSubject{
		SubjectObjectId:   foundSubject.SubjectId,
		Permissionship:    permissionship,
		PartialCaveatInfo: partialCaveat,
	}, nil
}

func normalizeSubjectRelation(sub *v1.SubjectReference) string {
	if sub.OptionalRelation == "" {
		return graph.Ellipsis
	}
	return sub.OptionalRelation
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
