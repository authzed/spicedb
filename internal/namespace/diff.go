package namespace

import (
	"fmt"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/scylladb/go-set/strset"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
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
)

// NamespaceDiff holds the diff between two namespaces.
type NamespaceDiff struct {
	existing *v0.NamespaceDefinition
	updated  *v0.NamespaceDefinition
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
	DirectType *v0.RelationReference
}

// DiffNamespaces performs a diff between two namespace definitions. One or both of the definitions
// can be `nil`, which will be treated as an add/remove as applicable.
func DiffNamespaces(existing *v0.NamespaceDefinition, updated *v0.NamespaceDefinition) (*NamespaceDiff, error) {
	// Check for the namespaces themselves.
	if existing == nil && updated == nil {
		return &NamespaceDiff{existing, updated, []Delta{}}, nil
	}

	if existing != nil && updated == nil {
		return &NamespaceDiff{
			existing: existing,
			updated:  updated,
			deltas: []Delta{
				Delta{
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
				Delta{
					Type: NamespaceAdded,
				},
			},
		}, nil
	}

	// Collect up relations and check.
	deltas := []Delta{}

	existingRels := map[string]*v0.Relation{}
	existingRelNames := strset.New()

	updatedRels := map[string]*v0.Relation{}
	updatedRelNames := strset.New()

	for _, relation := range existing.Relation {
		_, ok := existingRels[relation.Name]
		if ok {
			return nil, fmt.Errorf("found duplicate relation %s in existing namespace %s", relation.Name, existing.Name)
		}

		existingRels[relation.Name] = relation
		existingRelNames.Add(relation.Name)
	}

	for _, relation := range updated.Relation {
		_, ok := updatedRels[relation.Name]
		if ok {
			return nil, fmt.Errorf("found duplicate relation %s in updagted namespace %s", relation.Name, updated.Name)
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
			existingTypeInfo = &v0.TypeInformation{}
		}

		updatedTypeInfo := updatedRel.TypeInformation
		if updatedTypeInfo == nil {
			updatedTypeInfo = &v0.TypeInformation{}
		}

		existingAllowedRels := tuple.NewONRSet()
		updatedAllowedRels := tuple.NewONRSet()

		for _, existingAllowed := range existingTypeInfo.AllowedDirectRelations {
			existingAllowedRels.Add(&v0.ObjectAndRelation{
				Namespace: existingAllowed.Namespace,
				Relation:  existingAllowed.Relation,
				ObjectId:  "",
			})
		}

		for _, updatedAllowed := range updatedTypeInfo.AllowedDirectRelations {
			updatedAllowedRels.Add(&v0.ObjectAndRelation{
				Namespace: updatedAllowed.Namespace,
				Relation:  updatedAllowed.Relation,
				ObjectId:  "",
			})
		}

		for _, removed := range existingAllowedRels.Subtract(updatedAllowedRels).AsSlice() {
			deltas = append(deltas, Delta{
				Type:         RelationDirectTypeRemoved,
				RelationName: shared,
				DirectType: &v0.RelationReference{
					Namespace: removed.Namespace,
					Relation:  removed.Relation,
				},
			})
		}

		for _, added := range updatedAllowedRels.Subtract(existingAllowedRels).AsSlice() {
			deltas = append(deltas, Delta{
				Type:         RelationDirectTypeAdded,
				RelationName: shared,
				DirectType: &v0.RelationReference{
					Namespace: added.Namespace,
					Relation:  added.Relation,
				},
			})
		}
	}

	return &NamespaceDiff{
		existing: existing,
		updated:  updated,
		deltas:   deltas,
	}, nil
}
