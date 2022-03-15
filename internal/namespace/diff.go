package namespace

import (
	"fmt"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/scylladb/go-set/strset"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

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

	// ChangedRelationImpl indicates that the implementation of the relation has changed in some
	// way.
	ChangedRelationImpl DeltaType = "changed-relation-implementation"

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

// NamespaceDiff holds the diff between two namespaces.
type NamespaceDiff struct {
	existing *core.NamespaceDefinition
	updated  *core.NamespaceDefinition
	deltas   []Delta
}

// Deltas returns the deltas between the two namespaces.
func (nd NamespaceDiff) Deltas() []Delta {
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
func DiffNamespaces(existing *core.NamespaceDefinition, updated *core.NamespaceDefinition) (*NamespaceDiff, error) {
	// Check for the namespaces themselves.
	if existing == nil && updated == nil {
		return &NamespaceDiff{existing, updated, []Delta{}}, nil
	}

	if existing != nil && updated == nil {
		return &NamespaceDiff{
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
		return &NamespaceDiff{
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

	updatedRels := map[string]*core.Relation{}
	updatedRelNames := strset.New()

	for _, relation := range existing.Relation {
		_, ok := existingRels[relation.Name]
		if ok {
			return nil, fmt.Errorf("found duplicate relation %s in existing definition %s", relation.Name, existing.Name)
		}

		existingRels[relation.Name] = relation
		existingRelNames.Add(relation.Name)
	}

	for _, relation := range updated.Relation {
		_, ok := updatedRels[relation.Name]
		if ok {
			return nil, fmt.Errorf("found duplicate relation %s in updated definition %s", relation.Name, updated.Name)
		}

		updatedRels[relation.Name] = relation
		updatedRelNames.Add(relation.Name)
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

	for _, shared := range strset.Intersection(existingRelNames, updatedRelNames).List() {
		existingRel := existingRels[shared]
		updatedRel := updatedRels[shared]

		// Compare implementations.
		m := &jsonpb.Marshaler{}
		existingRewriteJSON, _ := m.MarshalToString(existingRel.UsersetRewrite)
		updatedRewriteJSON, _ := m.MarshalToString(updatedRel.UsersetRewrite)
		if existingRewriteJSON != updatedRewriteJSON {
			deltas = append(deltas, Delta{
				Type:         ChangedRelationImpl,
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

	return &NamespaceDiff{
		existing: existing,
		updated:  updated,
		deltas:   deltas,
	}, nil
}
