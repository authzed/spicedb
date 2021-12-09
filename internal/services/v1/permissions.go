package v1

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	dispatch "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

func (ps *permissionServer) CheckPermission(ctx context.Context, req *v1.CheckPermissionRequest) (*v1.CheckPermissionResponse, error) {
	atRevision, checkedAt := consistency.MustRevisionFromContext(ctx)

	err := ps.nsm.CheckNamespaceAndRelation(ctx, req.Resource.ObjectType, req.Permission, false, atRevision)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	err = ps.nsm.CheckNamespaceAndRelation(ctx,
		req.Subject.Object.ObjectType,
		normalizeSubjectRelation(req.Subject),
		true,
		atRevision,
	)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	cr, err := ps.dispatch.DispatchCheck(ctx, &dispatch.DispatchCheckRequest{
		Metadata: &dispatch.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: ps.defaultDepth,
		},
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: req.Resource.ObjectType,
			ObjectId:  req.Resource.ObjectId,
			Relation:  req.Permission,
		},
		Subject: &v0.ObjectAndRelation{
			Namespace: req.Subject.Object.ObjectType,
			ObjectId:  req.Subject.Object.ObjectId,
			Relation:  normalizeSubjectRelation(req.Subject),
		},
	})
	usagemetrics.SetInContext(ctx, cr.Metadata)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	var permissionship v1.CheckPermissionResponse_Permissionship
	switch cr.Membership {
	case dispatch.DispatchCheckResponse_MEMBER:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
	case dispatch.DispatchCheckResponse_NOT_MEMBER:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
	default:
		permissionship = v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED
	}

	return &v1.CheckPermissionResponse{
		CheckedAt:      checkedAt,
		Permissionship: permissionship,
	}, nil
}

func (ps *permissionServer) ExpandPermissionTree(ctx context.Context, req *v1.ExpandPermissionTreeRequest) (*v1.ExpandPermissionTreeResponse, error) {
	atRevision, expandedAt := consistency.MustRevisionFromContext(ctx)

	err := ps.nsm.CheckNamespaceAndRelation(ctx, req.Resource.ObjectType, req.Permission, false, atRevision)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	resp, err := ps.dispatch.DispatchExpand(ctx, &dispatch.DispatchExpandRequest{
		Metadata: &dispatch.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: ps.defaultDepth,
		},
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: req.Resource.ObjectType,
			ObjectId:  req.Resource.ObjectId,
			Relation:  req.Permission,
		},
		ExpansionMode: dispatch.DispatchExpandRequest_SHALLOW,
	})
	usagemetrics.SetInContext(ctx, resp.Metadata)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	// TODO(jschorr): Change to either using shared interfaces for nodes, or switch the internal
	// dispatched expand to return V1 node types.
	return &v1.ExpandPermissionTreeResponse{
		TreeRoot:   translateExpansionTree(resp.TreeNode),
		ExpandedAt: expandedAt,
	}, nil
}

// TranslateRelationshipTree translates a V1 PermissionRelationshipTree into a V0 RelationTupleTreeNode.
func TranslateRelationshipTree(tree *v1.PermissionRelationshipTree) *v0.RelationTupleTreeNode {
	var expanded *v0.ObjectAndRelation
	if tree.ExpandedObject != nil {
		expanded = &v0.ObjectAndRelation{
			Namespace: tree.ExpandedObject.ObjectType,
			ObjectId:  tree.ExpandedObject.ObjectId,
			Relation:  tree.ExpandedRelation,
		}
	}

	switch t := tree.TreeType.(type) {
	case *v1.PermissionRelationshipTree_Intermediate:
		operation := v0.SetOperationUserset_INVALID
		switch t.Intermediate.Operation {
		case v1.AlgebraicSubjectSet_OPERATION_EXCLUSION:
			operation = v0.SetOperationUserset_EXCLUSION
		case v1.AlgebraicSubjectSet_OPERATION_INTERSECTION:
			operation = v0.SetOperationUserset_INTERSECTION
		case v1.AlgebraicSubjectSet_OPERATION_UNION:
			operation = v0.SetOperationUserset_UNION
		default:
			panic("Unknown set operation")
		}

		children := []*v0.RelationTupleTreeNode{}
		for _, child := range t.Intermediate.Children {
			children = append(children, TranslateRelationshipTree(child))
		}

		return &v0.RelationTupleTreeNode{
			NodeType: &v0.RelationTupleTreeNode_IntermediateNode{
				IntermediateNode: &v0.SetOperationUserset{
					Operation:  operation,
					ChildNodes: children,
				},
			},
			Expanded: expanded,
		}

	case *v1.PermissionRelationshipTree_Leaf:
		var users []*v0.User
		for _, subj := range t.Leaf.Subjects {
			users = append(users, &v0.User{
				UserOneof: &v0.User_Userset{
					Userset: &v0.ObjectAndRelation{
						Namespace: subj.Object.ObjectType,
						ObjectId:  subj.Object.ObjectId,
						Relation:  stringz.DefaultEmpty(subj.OptionalRelation, "..."),
					},
				},
			})
		}

		return &v0.RelationTupleTreeNode{
			NodeType: &v0.RelationTupleTreeNode_LeafNode{
				LeafNode: &v0.DirectUserset{Users: users},
			},
			Expanded: expanded,
		}

	default:
		panic("Unknown type of expansion tree node")
	}
}

func translateExpansionTree(node *v0.RelationTupleTreeNode) *v1.PermissionRelationshipTree {
	switch t := node.NodeType.(type) {
	case *v0.RelationTupleTreeNode_IntermediateNode:
		operation := v1.AlgebraicSubjectSet_OPERATION_UNSPECIFIED

		switch t.IntermediateNode.Operation {
		case v0.SetOperationUserset_EXCLUSION:
			operation = v1.AlgebraicSubjectSet_OPERATION_EXCLUSION
		case v0.SetOperationUserset_INTERSECTION:
			operation = v1.AlgebraicSubjectSet_OPERATION_INTERSECTION
		case v0.SetOperationUserset_UNION:
			operation = v1.AlgebraicSubjectSet_OPERATION_UNION
		default:
			panic("Unknown set operation")
		}

		var children []*v1.PermissionRelationshipTree
		for _, child := range node.GetIntermediateNode().ChildNodes {
			children = append(children, translateExpansionTree(child))
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

	case *v0.RelationTupleTreeNode_LeafNode:
		var subjects []*v1.SubjectReference
		for _, found := range t.LeafNode.Users {
			subjects = append(subjects, &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: found.GetUserset().Namespace,
					ObjectId:   found.GetUserset().ObjectId,
				},
				OptionalRelation: denormalizeSubjectRelation(found.GetUserset().Relation),
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
		panic("Unknown type of expansion tree node")
	}
}

func (ps *permissionServer) LookupResources(req *v1.LookupResourcesRequest, resp v1.PermissionsService_LookupResourcesServer) error {
	ctx := resp.Context()
	atRevision, revisionReadAt := consistency.MustRevisionFromContext(ctx)

	err := ps.nsm.CheckNamespaceAndRelation(
		ctx,
		req.Subject.Object.ObjectType,
		normalizeSubjectRelation(req.Subject),
		true,
		atRevision,
	)
	if err != nil {
		return rewritePermissionsError(ctx, err)
	}

	err = ps.nsm.CheckNamespaceAndRelation(ctx, req.ResourceObjectType, req.Permission, false, atRevision)
	if err != nil {
		return rewritePermissionsError(ctx, err)
	}

	// TODO(jschorr): Change the internal dispatched lookup to also be streamed.
	lookupResp, err := ps.dispatch.DispatchLookup(ctx, &dispatch.DispatchLookupRequest{
		Metadata: &dispatch.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: ps.defaultDepth,
		},
		ObjectRelation: &v0.RelationReference{
			Namespace: req.ResourceObjectType,
			Relation:  req.Permission,
		},
		Subject: &v0.ObjectAndRelation{
			Namespace: req.Subject.Object.ObjectType,
			ObjectId:  req.Subject.Object.ObjectId,
			Relation:  normalizeSubjectRelation(req.Subject),
		},
		Limit:       ^uint32(0), // Set no limit for now
		DirectStack: nil,
		TtuStack:    nil,
	})
	usagemetrics.SetInContext(ctx, lookupResp.Metadata)
	if err != nil {
		return rewritePermissionsError(ctx, err)
	}

	for _, found := range lookupResp.ResolvedOnrs {
		if found.Namespace != req.ResourceObjectType {
			return rewritePermissionsError(
				ctx,
				fmt.Errorf("got invalid resolved object %v (expected %v)", found.Namespace, req.ResourceObjectType),
			)
		}

		err := resp.Send(&v1.LookupResourcesResponse{
			LookedUpAt:       revisionReadAt,
			ResourceObjectId: found.ObjectId,
		})
		if err != nil {
			return err
		}

	}
	return nil
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
