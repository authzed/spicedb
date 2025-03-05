package namespace

import (
	"github.com/google/go-cmp/cmp"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/testing/protocmp"

	nsinternal "github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/schema"
)

// DeltaType defines the type of namespace deltas.
type DeltaType string

const (
	// NamespaceAdded indicates that the namespace was newly added/created.
	NamespaceAdded DeltaType = "namespace-added"

	// NamespaceRemoved indicates that the namespace was removed.
	NamespaceRemoved DeltaType = "namespace-removed"

	// NamespaceCommentsChanged indicates that the comment(s) on the namespace were changed.
	NamespaceCommentsChanged DeltaType = "namespace-comments-changed"

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

	// ChangedPermissionComment indicates that the comment of the permission has changed in some way.
	ChangedPermissionComment DeltaType = "changed-permission-comment"

	// LegacyChangedRelationImpl indicates that the implementation of the relation has changed in some
	// way. This is for legacy checks and should not apply to any modern namespaces created
	// via schema.
	LegacyChangedRelationImpl DeltaType = "legacy-changed-relation-implementation"

	// RelationAllowedTypeAdded indicates that an allowed relation type has been added to
	// the relation.
	RelationAllowedTypeAdded DeltaType = "relation-allowed-type-added"

	// RelationAllowedTypeRemoved indicates that an allowed relation type has been removed from
	// the relation.
	RelationAllowedTypeRemoved DeltaType = "relation-allowed-type-removed"

	// ChangedRelationComment indicates that the comment of the relation has changed in some way.
	ChangedRelationComment DeltaType = "changed-relation-comment"
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

// Delta holds a single change of a namespace.
type Delta struct {
	// Type is the type of this delta.
	Type DeltaType

	// RelationName is the name of the relation to which this delta applies, if any.
	RelationName string

	// AllowedType is the allowed relation type added or removed, if any.
	AllowedType *core.AllowedRelation
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

	deltas := []Delta{}

	// Check the namespace's comments.
	existingComments := nspkg.GetComments(existing.Metadata)
	updatedComments := nspkg.GetComments(updated.Metadata)
	if !slices.Equal(existingComments, updatedComments) {
		deltas = append(deltas, Delta{
			Type: NamespaceCommentsChanged,
		})
	}

	// Collect up relations and check.
	existingRels := map[string]*core.Relation{}
	existingRelNames := mapz.NewSet[string]()

	existingPerms := map[string]*core.Relation{}
	existingPermNames := mapz.NewSet[string]()

	updatedRels := map[string]*core.Relation{}
	updatedRelNames := mapz.NewSet[string]()

	updatedPerms := map[string]*core.Relation{}
	updatedPermNames := mapz.NewSet[string]()

	for _, relation := range existing.Relation {
		_, ok := existingRels[relation.Name]
		if ok {
			return nil, nsinternal.NewDuplicateRelationError(existing.Name, relation.Name)
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
			return nil, nsinternal.NewDuplicateRelationError(updated.Name, relation.Name)
		}

		if isPermission(relation) {
			updatedPerms[relation.Name] = relation
			updatedPermNames.Add(relation.Name)
		} else {
			updatedRels[relation.Name] = relation
			updatedRelNames.Add(relation.Name)
		}
	}

	_ = existingRelNames.Subtract(updatedRelNames).ForEach(func(removed string) error {
		deltas = append(deltas, Delta{
			Type:         RemovedRelation,
			RelationName: removed,
		})
		return nil
	})

	_ = updatedRelNames.Subtract(existingRelNames).ForEach(func(added string) error {
		deltas = append(deltas, Delta{
			Type:         AddedRelation,
			RelationName: added,
		})
		return nil
	})

	_ = existingPermNames.Subtract(updatedPermNames).ForEach(func(removed string) error {
		deltas = append(deltas, Delta{
			Type:         RemovedPermission,
			RelationName: removed,
		})
		return nil
	})

	_ = updatedPermNames.Subtract(existingPermNames).ForEach(func(added string) error {
		deltas = append(deltas, Delta{
			Type:         AddedPermission,
			RelationName: added,
		})
		return nil
	})

	_ = existingPermNames.Intersect(updatedPermNames).ForEach(func(shared string) error {
		existingPerm := existingPerms[shared]
		updatedPerm := updatedPerms[shared]

		// Compare implementations.
		if areDifferentExpressions(existingPerm.UsersetRewrite, updatedPerm.UsersetRewrite) {
			deltas = append(deltas, Delta{
				Type:         ChangedPermissionImpl,
				RelationName: shared,
			})
		}

		// Compare comments.
		existingComments := nspkg.GetComments(existingPerm.Metadata)
		updatedComments := nspkg.GetComments(updatedPerm.Metadata)
		if !slices.Equal(existingComments, updatedComments) {
			deltas = append(deltas, Delta{
				Type:         ChangedPermissionComment,
				RelationName: shared,
			})
		}
		return nil
	})

	_ = existingRelNames.Intersect(updatedRelNames).ForEach(func(shared string) error {
		existingRel := existingRels[shared]
		updatedRel := updatedRels[shared]

		// Compare implementations (legacy).
		if areDifferentExpressions(existingRel.UsersetRewrite, updatedRel.UsersetRewrite) {
			deltas = append(deltas, Delta{
				Type:         LegacyChangedRelationImpl,
				RelationName: shared,
			})
		}

		// Compare comments.
		existingComments := nspkg.GetComments(existingRel.Metadata)
		updatedComments := nspkg.GetComments(updatedRel.Metadata)
		if !slices.Equal(existingComments, updatedComments) {
			deltas = append(deltas, Delta{
				Type:         ChangedRelationComment,
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

		existingAllowedRels := mapz.NewSet[string]()
		updatedAllowedRels := mapz.NewSet[string]()
		allowedRelsBySource := map[string]*core.AllowedRelation{}

		for _, existingAllowed := range existingTypeInfo.AllowedDirectRelations {
			source := schema.SourceForAllowedRelation(existingAllowed)
			allowedRelsBySource[source] = existingAllowed
			existingAllowedRels.Add(source)
		}

		for _, updatedAllowed := range updatedTypeInfo.AllowedDirectRelations {
			source := schema.SourceForAllowedRelation(updatedAllowed)
			allowedRelsBySource[source] = updatedAllowed
			updatedAllowedRels.Add(source)
		}

		_ = existingAllowedRels.Subtract(updatedAllowedRels).ForEach(func(removed string) error {
			deltas = append(deltas, Delta{
				Type:         RelationAllowedTypeRemoved,
				RelationName: shared,
				AllowedType:  allowedRelsBySource[removed],
			})
			return nil
		})

		_ = updatedAllowedRels.Subtract(existingAllowedRels).ForEach(func(added string) error {
			deltas = append(deltas, Delta{
				Type:         RelationAllowedTypeAdded,
				RelationName: shared,
				AllowedType:  allowedRelsBySource[added],
			})
			return nil
		})

		return nil
	})

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
	// Return whether the rewrites are different, ignoring the SourcePosition message type.
	delta := cmp.Diff(
		existing,
		updated,
		protocmp.Transform(),
		protocmp.IgnoreMessages(&core.SourcePosition{}),
	)
	return delta != ""
}
