package namespace

import (
	"fmt"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/scylladb/go-set/strset"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"

	nspkg "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DeltaType defines the type of namespace deltas.
type DeltaType string

const (
	// NamespaceAdded indicates that the namespace was newly added/created.
	NamespaceAdded DeltaType = "namespace-added"

	// NamespaceRemoved indicates that the namespace was removed.
	NamespaceRemoved DeltaType = "namespace-removed"

	// AddedRelation indicates that the relation was added to the namespace.
	AddedRelation DeltaType = "added-relation"

	// RemovedRelation indicates that the relation was removed from the namespace.
	RemovedRelation DeltaType = "removed-relation"

	// AddedPermission indicates that the permission was added to the namespace.
	AddedPermission DeltaType = "added-permission"

	// RemovedPermission indicates that the permission was removed from the namespace.
	RemovedPermission DeltaType = "removed-permission"

	// ChangedPermissionImpl indicates that the implementation of the permission has changed in some
	// way.
	ChangedPermissionImpl DeltaType = "changed-permission-implementation"

	// LegacyChangedRelationImpl indicates that the implementation of the relation has changed in some
	// way. This is for legacy checks and should not apply to any modern namespaces created
	// via schema.
	LegacyChangedRelationImpl DeltaType = "legacy-changed-relation-implementation"

	// RelationDirectTypeAdded indicates that an allowed direct relation type has been added to
	// the relation.
	RelationDirectTypeAdded DeltaType = "relation-direct-type-added"

	// RelationDirectTypeRemoved indicates that an allowed direct relation type has been removed from
	// the relation.
	RelationDirectTypeRemoved DeltaType = "relation-direct-type-removed"

	// RelationDirectWildcardTypeAdded indicates that an allowed relation wildcard type has been added to
	// the relation.
	RelationDirectWildcardTypeAdded DeltaType = "relation-wildcard-type-added"

	// RelationDirectWildcardTypeRemoved indicates that an allowed relation wildcard type has been removed from
	// the relation.
	RelationDirectWildcardTypeRemoved DeltaType = "relation-wildcard-type-removed"
)

// Diff holds the diff between two namespaces.
type Diff struct {
	existing *core.NamespaceDefinition
	updated  *core.NamespaceDefinition
	deltas   []Delta
}

// Deltas returns the deltas between the two namespaces.
func (nd Diff) Deltas() []Delta {
	return nd.deltas
}

type Delta struct {
	// Type is the type of this delta.
	Type DeltaType

	// RelationName is the name of the relation to which this delta applies, if any.
	RelationName string

	// DirectType is the direct relation type added or removed, if any.
	DirectType *core.RelationReference

	// WildcardType is the wildcard type added or removed, if any.
	WildcardType string
}

// DiffNamespaces performs a diff between two namespace definitions. One or both of the definitions
// can be `nil`, which will be treated as an add/remove as applicable.
func DiffNamespaces(existing *core.NamespaceDefinition, updated *core.NamespaceDefinition) (*Diff, error) {
	// Check for the namespaces themselves.
	if existing == nil && updated == nil {
		return &Diff{existing, updated, []Delta{}}, nil
	}

	if existing != nil && updated == nil {
		return &Diff{
			existing: existing,
			updated:  updated,
			deltas: []Delta{
				{
					Type: NamespaceRemoved,
				},
			},
		}, nil
	}

	if existing == nil && updated != nil {
		return &Diff{
			existing: existing,
			updated:  updated,
			deltas: []Delta{
				{
					Type: NamespaceAdded,
				},
			},
		}, nil
	}

	// Collect up relations and check.
	deltas := []Delta{}

	existingRels := map[string]*core.Relation{}
	existingRelNames := strset.New()

	existingPerms := map[string]*core.Relation{}
	existingPermNames := strset.New()

	updatedRels := map[string]*core.Relation{}
	updatedRelNames := strset.New()

	updatedPerms := map[string]*core.Relation{}
	updatedPermNames := strset.New()

	for _, relation := range existing.Relation {
		_, ok := existingRels[relation.Name]
		if ok {
			return nil, fmt.Errorf("found duplicate relation %s in existing definition %s", relation.Name, existing.Name)
		}

		if isPermission(relation) {
			existingPerms[relation.Name] = relation
			existingPermNames.Add(relation.Name)
		} else {
			existingRels[relation.Name] = relation
			existingRelNames.Add(relation.Name)
		}
	}

	for _, relation := range updated.Relation {
		_, ok := updatedRels[relation.Name]
		if ok {
			return nil, fmt.Errorf("found duplicate relation %s in updated definition %s", relation.Name, updated.Name)
		}

		if isPermission(relation) {
			updatedPerms[relation.Name] = relation
			updatedPermNames.Add(relation.Name)
		} else {
			updatedRels[relation.Name] = relation
			updatedRelNames.Add(relation.Name)
		}
	}

	for _, removed := range strset.Difference(existingRelNames, updatedRelNames).List() {
		deltas = append(deltas, Delta{
			Type:         RemovedRelation,
			RelationName: removed,
		})
	}

	for _, added := range strset.Difference(updatedRelNames, existingRelNames).List() {
		deltas = append(deltas, Delta{
			Type:         AddedRelation,
			RelationName: added,
		})
	}

	for _, removed := range strset.Difference(existingPermNames, updatedPermNames).List() {
		deltas = append(deltas, Delta{
			Type:         RemovedPermission,
			RelationName: removed,
		})
	}

	for _, added := range strset.Difference(updatedPermNames, existingPermNames).List() {
		deltas = append(deltas, Delta{
			Type:         AddedPermission,
			RelationName: added,
		})
	}

	for _, shared := range strset.Intersection(existingPermNames, updatedPermNames).List() {
		existingPerm := existingPerms[shared]
		updatedPerm := updatedPerms[shared]

		// Compare implementations.
		if areDifferentExpressions(existingPerm.UsersetRewrite, updatedPerm.UsersetRewrite) {
			deltas = append(deltas, Delta{
				Type:         ChangedPermissionImpl,
				RelationName: shared,
			})
		}
	}

	for _, shared := range strset.Intersection(existingRelNames, updatedRelNames).List() {
		existingRel := existingRels[shared]
		updatedRel := updatedRels[shared]

		// Compare implementations (legacy).
		if areDifferentExpressions(existingRel.UsersetRewrite, updatedRel.UsersetRewrite) {
			deltas = append(deltas, Delta{
				Type:         LegacyChangedRelationImpl,
				RelationName: shared,
			})
		}

		// Compare type information.
		existingTypeInfo := existingRel.TypeInformation
		if existingTypeInfo == nil {
			existingTypeInfo = &core.TypeInformation{}
		}

		updatedTypeInfo := updatedRel.TypeInformation
		if updatedTypeInfo == nil {
			updatedTypeInfo = &core.TypeInformation{}
		}

		existingAllowedRels := tuple.NewONRSet()
		updatedAllowedRels := tuple.NewONRSet()

		for _, existingAllowed := range existingTypeInfo.AllowedDirectRelations {
			if existingAllowed.GetRelation() != "" {
				existingAllowedRels.Add(&core.ObjectAndRelation{
					Namespace: existingAllowed.Namespace,
					Relation:  existingAllowed.GetRelation(),
					ObjectId:  "",
				})
			}

			if existingAllowed.GetPublicWildcard() != nil {
				existingAllowedRels.Add(&core.ObjectAndRelation{
					Namespace: existingAllowed.Namespace,
					Relation:  "-",
					ObjectId:  tuple.PublicWildcard,
				})
			}
		}

		for _, updatedAllowed := range updatedTypeInfo.AllowedDirectRelations {
			if updatedAllowed.GetRelation() != "" {
				updatedAllowedRels.Add(&core.ObjectAndRelation{
					Namespace: updatedAllowed.Namespace,
					Relation:  updatedAllowed.GetRelation(),
					ObjectId:  "",
				})
			}

			if updatedAllowed.GetPublicWildcard() != nil {
				updatedAllowedRels.Add(&core.ObjectAndRelation{
					Namespace: updatedAllowed.Namespace,
					Relation:  "-",
					ObjectId:  tuple.PublicWildcard,
				})
			}
		}

		for _, removed := range existingAllowedRels.Subtract(updatedAllowedRels).AsSlice() {
			if removed.ObjectId == tuple.PublicWildcard {
				deltas = append(deltas, Delta{
					Type:         RelationDirectWildcardTypeRemoved,
					RelationName: shared,
					WildcardType: removed.Namespace,
				})
			} else {
				deltas = append(deltas, Delta{
					Type:         RelationDirectTypeRemoved,
					RelationName: shared,
					DirectType: &core.RelationReference{
						Namespace: removed.Namespace,
						Relation:  removed.Relation,
					},
				})
			}
		}

		for _, added := range updatedAllowedRels.Subtract(existingAllowedRels).AsSlice() {
			if added.ObjectId == tuple.PublicWildcard {
				deltas = append(deltas, Delta{
					Type:         RelationDirectWildcardTypeAdded,
					RelationName: shared,
					WildcardType: added.Namespace,
				})
			} else {
				deltas = append(deltas, Delta{
					Type:         RelationDirectTypeAdded,
					RelationName: shared,
					DirectType: &core.RelationReference{
						Namespace: added.Namespace,
						Relation:  added.Relation,
					},
				})
			}
		}
	}

	return &Diff{
		existing: existing,
		updated:  updated,
		deltas:   deltas,
	}, nil
}

func isPermission(relation *core.Relation) bool {
	return nspkg.GetRelationKind(relation) == iv1.RelationMetadata_PERMISSION
}

func areDifferentExpressions(existing *core.UsersetRewrite, updated *core.UsersetRewrite) bool {
	m := &jsonpb.Marshaler{}
	existingRewriteJSON := ""
	updatedRewriteJSON := ""
	var merr error

	if existing != nil {
		existingRewriteJSON, merr = m.MarshalToString(existing)
		if merr != nil {
			panic(fmt.Sprintf("got error in marshaling rewrite: %v", merr))
		}
	}

	if updated != nil {
		updatedRewriteJSON, merr = m.MarshalToString(updated)
		if merr != nil {
			panic(fmt.Sprintf("got error in marshaling rewrite: %v", merr))
		}
	}

	return existingRewriteJSON != updatedRewriteJSON
}
