package testing

import (
	"context"
	"errors"
	"iter"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// CheckWithSchemaVariant runs the provided handler with a randomly generated schema and a
// randomly generated variant of it. The relationship generator given is for the base schema.
func CheckWithSchemaVariant(t *testing.T, handler func(t *rapid.T, variant Variant, baseRelationshipGenerator RelationshipGenerator), opts ...VariantOption) {
	t.Helper()
	rapid.Check(t, func(t *rapid.T) {
		base, baseRelationshipGenerator := GenerateSchema(t)
		handler(t, GenerateVariant(t, base, opts...), baseRelationshipGenerator)
	})
}

// VariantOption is an option for the generation of a variant.
type VariantOption func(*variantOptions)

type variantOptions struct {
	// kindWeights are the weights of the mutation kinds. A kind without a weight has a
	// weight of 1 if allKinds is set, and 0 otherwise.
	kindWeights map[MutationKind]int
	allKinds    bool

	minCount, maxCount int

	relationUnionChildren bool
}

// WithMutationKinds restricts the mutations of the variant to the given kinds. A mutation
// that would require a mutation of another kind is not made: a relation used by a
// permission is not removed unless ChangePermission is also given, for example. If none of
// the kinds can be applied to the schema, the test case is skipped.
func WithMutationKinds(kinds ...MutationKind) VariantOption {
	return func(opts *variantOptions) {
		opts.allKinds = false
		opts.kindWeights = make(map[MutationKind]int, len(kinds))
		for _, kind := range kinds {
			opts.kindWeights[kind] = 1
		}
	}
}

// WithMutationKindWeight sets how likely the kind of mutation is to be chosen, relative to
// the others, each of which has a weight of 1 by default. A weight of 0 excludes the kind,
// in the same manner as WithMutationKinds.
func WithMutationKindWeight(kind MutationKind, weight int) VariantOption {
	return func(opts *variantOptions) {
		opts.kindWeights[kind] = weight
	}
}

// WithMutationCount sets the minimum and maximum number of mutations chosen for the variant,
// which are 1 and 4 by default. The variant can contain more, as it also contains the
// mutations those chosen require, and can contain fewer if no more can be applied.
func WithMutationCount(minCount, maxCount int) VariantOption {
	return func(opts *variantOptions) {
		opts.minCount = minCount
		opts.maxCount = maxCount
	}
}

// WithRelationUnionChildren has every AddUnionChild add a single relation, rather than a
// randomly generated expression.
func WithRelationUnionChildren() VariantOption {
	return func(opts *variantOptions) {
		opts.relationUnionChildren = true
	}
}

// weight returns the weight of the mutation kind.
func (vo *variantOptions) weight(kind MutationKind) int {
	if vo == nil {
		return 1
	}
	if weight, ok := vo.kindWeights[kind]; ok {
		return weight
	}
	if vo.allKinds {
		return 1
	}
	return 0
}

// GenerateVariant generates a variant of the base schema, by applying between one and four
// randomly chosen mutations to it, along with any further mutations they require. The result
// is always a valid schema. To generate changes to a schema over time, call it again with the
// Schema of the returned variant. To generate a change being rolled back, use the Inverse of
// the returned variant.
//
// The mutations chosen can be controlled with options: see WithMutationKinds,
// WithMutationKindWeight, WithMutationCount and WithRelationUnionChildren.
func GenerateVariant(t *rapid.T, base *schema.Schema, opts ...VariantOption) Variant {
	if err := checkSupported(base); err != nil {
		t.Fatalf("cannot generate variant: %v", err)
	}

	options := &variantOptions{kindWeights: map[MutationKind]int{}, allKinds: true, minCount: 1, maxCount: 4}
	for _, opt := range opts {
		opt(options)
	}

	if options.minCount < 1 || options.maxCount < options.minCount {
		t.Fatalf("cannot generate variant: invalid mutation count of %d to %d", options.minCount, options.maxCount)
	}
	for kind, weight := range options.kindWeights {
		if weight < 0 {
			t.Fatalf("cannot generate variant: invalid weight of %d for %s", weight, kind)
		}
	}

	vb := &variantBuilder{
		t:                   t,
		options:             options,
		working:             modelFromSchema(base),
		targetedNames:       map[memberTarget]struct{}{},
		targetedDefinitions: map[string]struct{}{},
	}

	count := rapid.IntRange(options.minCount, options.maxCount).Draw(t, "mutationCount")
	for i := range count {
		vb.label = "mutation#" + strconv.Itoa(i) + "::"
		if !vb.addMutation() {
			break
		}
	}

	if len(vb.mutations) == 0 {
		t.Skipf("none of the mutation kinds allowed can be applied to the schema")
	}

	variant, err := NewVariant(base, vb.mutations)
	require.NoError(t, err)
	return variant
}

// memberTarget is a relation or permission in a definition.
type memberTarget struct{ definition, name string }

// allowedTypeTarget is a relation along with the allowed types that can be added to it.
type allowedTypeTarget struct {
	memberTarget
	options []AllowedType
}

// variantBuilder holds the state of a variant being generated.
type variantBuilder struct {
	t *rapid.T

	// options are the options for the variant, if any.
	options *variantOptions

	// working is the base schema with the mutations so far applied.
	working schemaModel

	// targetedNames are the relations and permissions already changed by a mutation.
	targetedNames map[memberTarget]struct{}

	// targetedDefinitions are the definitions already added or removed by a mutation.
	targetedDefinitions map[string]struct{}

	mutations []Mutation

	// label is the prefix for the labels of the draws of the current mutation.
	label string
}

// addMutation draws a single mutation from those that can currently be applied, and applies
// it along with any mutations it requires. It returns false if there are none.
func (vb *variantBuilder) addMutation() bool {
	removableDefinitions := vb.removableDefinitions()
	relationDefinitions := vb.freeDefinitions()
	removableRelations := vb.removableMembers(RemoveRelation)
	addableAllowedTypes := vb.addableAllowedTypes()
	removableAllowedTypes := vb.removableAllowedTypes()
	permissionDefinitions := vb.permissionDefinitions()
	removablePermissions := vb.removableMembers(RemovePermission)
	changeablePermissions := vb.changeablePermissions()
	unionPermissions := vb.unionPermissions()

	// NOTE: a kind is found once for each unit of its weight. Adding a definition is always
	// possible.
	var kinds []MutationKind
	addIf := func(kind MutationKind, applicable bool) {
		if applicable {
			kinds = append(kinds, slices.Repeat([]MutationKind{kind}, vb.options.weight(kind))...)
		}
	}
	addIf(AddDefinition, true)
	addIf(RemoveDefinition, len(removableDefinitions) > 0)
	addIf(AddRelation, len(relationDefinitions) > 0)
	addIf(RemoveRelation, len(removableRelations) > 0)
	addIf(AddAllowedType, len(addableAllowedTypes) > 0)
	addIf(RemoveAllowedType, len(removableAllowedTypes) > 0)
	addIf(AddPermission, len(permissionDefinitions) > 0)
	addIf(RemovePermission, len(removablePermissions) > 0)
	addIf(ChangePermission, len(changeablePermissions) > 0)
	addIf(AddUnionChild, len(changeablePermissions) > 0)
	addIf(RemoveUnionChild, len(unionPermissions) > 0)

	if len(kinds) == 0 {
		return false
	}

	t := vb.t
	switch rapid.SampledFrom(kinds).Draw(t, vb.label+"kind") {
	case AddDefinition:
		vb.record(vb.generateDefinition())

	case RemoveDefinition:
		vb.record(Mutation{
			Kind:       RemoveDefinition,
			Definition: rapid.SampledFrom(removableDefinitions).Draw(t, vb.label+"definition"),
		})

	case AddRelation:
		definitionName := rapid.SampledFrom(relationDefinitions).Draw(t, vb.label+"definition")
		vb.record(Mutation{
			Kind:         AddRelation,
			Definition:   definitionName,
			Name:         vb.newName("r_", definitionName, "relationName"),
			AllowedTypes: vb.generateAllowedTypes(vb.working, "allowedTypes"),
		})

	case RemoveRelation:
		target := rapid.SampledFrom(removableRelations).Draw(t, vb.label+"relation")
		vb.record(vb.removeMember(RemoveRelation, target)...)

	case AddAllowedType:
		target := rapid.SampledFrom(addableAllowedTypes).Draw(t, vb.label+"relation")
		vb.record(Mutation{
			Kind:         AddAllowedType,
			Definition:   target.definition,
			Name:         target.name,
			AllowedTypes: []AllowedType{rapid.SampledFrom(target.options).Draw(t, vb.label+"allowedType")},
		})

	case RemoveAllowedType:
		target := rapid.SampledFrom(removableAllowedTypes).Draw(t, vb.label+"relation")
		allowedTypes := vb.working[target.definition].relations[target.name]
		vb.record(Mutation{
			Kind:         RemoveAllowedType,
			Definition:   target.definition,
			Name:         target.name,
			AllowedTypes: []AllowedType{rapid.SampledFrom(allowedTypes).Draw(t, vb.label+"allowedType")},
		})

	case AddPermission:
		definitionName := rapid.SampledFrom(permissionDefinitions).Draw(t, vb.label+"definition")
		vb.record(Mutation{
			Kind:       AddPermission,
			Definition: definitionName,
			Name:       vb.newName("p_", definitionName, "permissionName"),
			Operation:  vb.generateOperation(vb.working, definitionName, "operation"),
		})

	case RemovePermission:
		target := rapid.SampledFrom(removablePermissions).Draw(t, vb.label+"permission")
		vb.record(vb.removeMember(RemovePermission, target)...)

	case ChangePermission:
		target := rapid.SampledFrom(changeablePermissions).Draw(t, vb.label+"permission")
		existing := vb.working[target.definition].permissions[target.name]
		changed := rapid.Custom(func(t *rapid.T) schema.Operation {
			return generateOperationFor(t, vb.working, target.definition, 3, vb.label+"operation")
		}).Filter(func(op schema.Operation) bool {
			return !operationsEquivalent(existing, op)
		}).Draw(t, vb.label+"changedOperation")

		vb.record(Mutation{
			Kind:       ChangePermission,
			Definition: target.definition,
			Name:       target.name,
			Operation:  changed,
		})

	case AddUnionChild:
		target := rapid.SampledFrom(changeablePermissions).Draw(t, vb.label+"permission")
		vb.record(Mutation{
			Kind:       AddUnionChild,
			Definition: target.definition,
			Name:       target.name,
			Operation:  vb.generateUnionChild(target.definition),
		})

	case RemoveUnionChild:
		target := rapid.SampledFrom(unionPermissions).Draw(t, vb.label+"permission")
		children := unionChildren(vb.working[target.definition].permissions[target.name])
		vb.record(Mutation{
			Kind:       RemoveUnionChild,
			Definition: target.definition,
			Name:       target.name,
			Operation:  children[rapid.IntRange(0, len(children)-1).Draw(t, vb.label+"childIndex")],
		})
	}

	return true
}

// generateUnionChild generates the child to add to the union of a permission in the definition.
func (vb *variantBuilder) generateUnionChild(definitionName string) schema.Operation {
	if vb.options != nil && vb.options.relationUnionChildren {
		relationNames := sortedKeys(vb.working[definitionName].relations)
		return schema.NewRelationRef(rapid.SampledFrom(relationNames).Draw(vb.t, vb.label+"childRelation"))
	}
	return generateOperationFor(vb.t, vb.working, definitionName, 1, vb.label+"child")
}

// record applies the mutations to the working schema and marks their targets as used.
func (vb *variantBuilder) record(mutations ...Mutation) {
	for _, mutation := range mutations {
		require.NoError(vb.t, vb.working.apply(mutation))

		switch mutation.Kind {
		case AddDefinition, RemoveDefinition:
			vb.targetedDefinitions[mutation.Definition] = struct{}{}
		default:
			vb.targetedNames[memberTarget{mutation.Definition, mutation.Name}] = struct{}{}
		}
	}
	vb.mutations = append(vb.mutations, mutations...)
}

// isFree returns whether the relation or permission can still be targeted by a mutation.
func (vb *variantBuilder) isFree(target memberTarget) bool {
	_, definitionTargeted := vb.targetedDefinitions[target.definition]
	_, nameTargeted := vb.targetedNames[target]
	return !definitionTargeted && !nameTargeted
}

// freeDefinitions returns the definitions that have not been added by a mutation, and
// can therefore have their relations and permissions changed.
func (vb *variantBuilder) freeDefinitions() []string {
	free := make([]string, 0, len(vb.working))
	for _, definitionName := range sortedKeys(vb.working) {
		if _, ok := vb.targetedDefinitions[definitionName]; !ok {
			free = append(free, definitionName)
		}
	}
	return free
}

// permissionDefinitions returns the definitions to which a permission can be added.
func (vb *variantBuilder) permissionDefinitions() []string {
	return slices.DeleteFunc(vb.freeDefinitions(), func(definitionName string) bool {
		return len(vb.working[definitionName].relations) == 0
	})
}

// removableDefinitions returns the definitions that can be removed: those that have not
// been otherwise changed and that no other definition makes use of as a subject type.
func (vb *variantBuilder) removableDefinitions() []string {
	if len(vb.working) < 2 {
		return nil
	}

	return slices.DeleteFunc(vb.freeDefinitions(), func(definitionName string) bool {
		for target := range vb.targetedNames {
			if target.definition == definitionName {
				return true
			}
		}

		for otherName, other := range vb.working {
			if otherName == definitionName {
				continue
			}
			for _, allowedTypes := range other.relations {
				for _, allowedType := range allowedTypes {
					if allowedType.Type == definitionName {
						return true
					}
				}
			}
		}
		return false
	})
}

// removableAllowedTypes returns the relations from which an allowed type can be removed.
func (vb *variantBuilder) removableAllowedTypes() []memberTarget {
	var targets []memberTarget
	for _, definitionName := range vb.freeDefinitions() {
		definition := vb.working[definitionName]
		for _, relationName := range sortedKeys(definition.relations) {
			target := memberTarget{definitionName, relationName}
			if vb.isFree(target) && len(definition.relations[relationName]) > 1 {
				targets = append(targets, target)
			}
		}
	}
	return targets
}

// addableAllowedTypes returns the relations to which an allowed type can be added, along
// with the allowed types that can be added to each. Arrows over a relation must continue
// to find their target on every subject type of the relation.
func (vb *variantBuilder) addableAllowedTypes() []allowedTypeTarget {
	subjectTypeNames := subjectTypeNamesFor(vb.working)

	var targets []allowedTypeTarget
	for _, definitionName := range vb.freeDefinitions() {
		definition := vb.working[definitionName]
		for _, relationName := range sortedKeys(definition.relations) {
			target := memberTarget{definitionName, relationName}
			if !vb.isFree(target) {
				continue
			}

			var arrowTargets []string
			for _, operation := range definition.permissions {
				walkOperation(operation, func(op schema.Operation) {
					if left, right, ok := arrowNames(op); ok && left == relationName {
						arrowTargets = append(arrowTargets, right)
					}
				})
			}

			var options []AllowedType
			for _, subjectTypeName := range subjectTypeNames {
				subjectDefinition := vb.working[subjectTypeName]
				if slices.ContainsFunc(arrowTargets, func(arrowTarget string) bool {
					return !subjectDefinition.hasName(arrowTarget)
				}) {
					continue
				}

				candidates := []AllowedType{{Type: subjectTypeName, Subrelation: tuple.Ellipsis}}
				for _, subjectRelationName := range sortedKeys(subjectDefinition.relations) {
					candidates = append(candidates, AllowedType{Type: subjectTypeName, Subrelation: subjectRelationName})
				}

				for _, candidate := range candidates {
					if !slices.Contains(definition.relations[relationName], candidate) {
						options = append(options, candidate)
					}
				}
			}

			if len(options) > 0 {
				targets = append(targets, allowedTypeTarget{target, options})
			}
		}
	}
	return targets
}

// changeablePermissions returns the permissions whose expression can be replaced.
func (vb *variantBuilder) changeablePermissions() []memberTarget {
	var targets []memberTarget
	for _, definitionName := range vb.permissionDefinitions() {
		for _, permissionName := range sortedKeys(vb.working[definitionName].permissions) {
			if target := (memberTarget{definitionName, permissionName}); vb.isFree(target) {
				targets = append(targets, target)
			}
		}
	}
	return targets
}

// unionPermissions returns the permissions from which a child of a union can be removed.
func (vb *variantBuilder) unionPermissions() []memberTarget {
	var targets []memberTarget
	for _, definitionName := range vb.freeDefinitions() {
		definition := vb.working[definitionName]
		for _, permissionName := range sortedKeys(definition.permissions) {
			target := memberTarget{definitionName, permissionName}
			if vb.isFree(target) && len(unionChildren(definition.permissions[permissionName])) > 1 {
				targets = append(targets, target)
			}
		}
	}
	return targets
}

// removableMembers returns the relations (for RemoveRelation) or permissions (for
// RemovePermission) that can be removed.
func (vb *variantBuilder) removableMembers(kind MutationKind) []memberTarget {
	var targets []memberTarget
	for _, definitionName := range vb.freeDefinitions() {
		definition := vb.working[definitionName]

		var names []string
		if kind == RemoveRelation {
			names = sortedKeys(definition.relations)
		} else {
			names = sortedKeys(definition.permissions)
		}

		for _, name := range names {
			if target := (memberTarget{definitionName, name}); vb.canRemoveMember(kind, target) {
				targets = append(targets, target)
			}
		}
	}
	return targets
}

// memberUse describes how a permission makes use of a relation or permission being removed.
type memberUse struct {
	// direct indicates the permission references the member by name, either as a relation
	// reference or as the left side of an arrow.
	direct bool

	// arrowTarget indicates the permission has an arrow that reaches the member.
	arrowTarget bool
}

// useOf returns how the permission's operation makes use of the member.
func (vb *variantBuilder) useOf(definitionName string, operation schema.Operation, member memberTarget) memberUse {
	var use memberUse
	walkOperation(operation, func(op schema.Operation) {
		if relationName, ok := relationReferenceName(op); ok {
			use.direct = use.direct || (definitionName == member.definition && relationName == member.name)
			return
		}

		left, right, ok := arrowNames(op)
		if !ok {
			return
		}

		switch {
		case definitionName == member.definition && left == member.name:
			use.direct = true
		case right == member.name && vb.reaches(definitionName, left, member.definition):
			use.arrowTarget = true
		}
	})
	return use
}

// reaches returns whether the relation allows subjects of the given type.
func (vb *variantBuilder) reaches(definitionName, relationName, subjectTypeName string) bool {
	return slices.ContainsFunc(vb.working[definitionName].relations[relationName], func(allowedType AllowedType) bool {
		return allowedType.Type == subjectTypeName
	})
}

// canRemoveMember returns whether the relation or permission can be removed, along with
// everything that makes use of it, without touching anything already changed.
func (vb *variantBuilder) canRemoveMember(kind MutationKind, member memberTarget) bool {
	if !vb.isFree(member) {
		return false
	}

	definition := vb.working[member.definition]
	substitutes := len(definition.relations)
	if kind == RemoveRelation {
		substitutes--

		// A definition can only be left without relations if it has no permissions.
		if substitutes == 0 && len(definition.permissions) > 0 {
			return false
		}
	}

	subjectType := AllowedType{Type: member.definition, Subrelation: member.name}
	for otherName, other := range vb.working {
		for relationName, allowedTypes := range other.relations {
			target := memberTarget{otherName, relationName}
			if target == member || !slices.Contains(allowedTypes, subjectType) {
				continue
			}

			// A permission cannot be removed while it is used as a subject relation. For a
			// relation, the allowed type is removed along with it.
			if kind == RemovePermission || !vb.isFree(target) || len(allowedTypes) < 2 {
				return false
			}
			if vb.options.weight(RemoveAllowedType) == 0 {
				return false
			}
		}

		for permissionName, operation := range other.permissions {
			target := memberTarget{otherName, permissionName}
			if target == member {
				continue
			}

			use := vb.useOf(otherName, operation, member)
			if !use.direct && !use.arrowTarget {
				continue
			}

			// A permission cannot be removed while it is the target of an arrow. For a
			// relation, the arrow is rewritten.
			if use.arrowTarget && kind == RemovePermission {
				return false
			}
			if use.direct && substitutes == 0 {
				return false
			}
			if !vb.isFree(target) || vb.options.weight(ChangePermission) == 0 {
				return false
			}
		}
	}

	return true
}

// removeMember returns the mutation removing the relation or permission, followed by the
// mutations for everything that made use of it.
func (vb *variantBuilder) removeMember(kind MutationKind, member memberTarget) []Mutation {
	mutations := []Mutation{{Kind: kind, Definition: member.definition, Name: member.name}}

	subjectType := AllowedType{Type: member.definition, Subrelation: member.name}
	for _, otherName := range sortedKeys(vb.working) {
		other := vb.working[otherName]
		for _, relationName := range sortedKeys(other.relations) {
			if (memberTarget{otherName, relationName}) == member || !slices.Contains(other.relations[relationName], subjectType) {
				continue
			}

			mutations = append(mutations, Mutation{
				Kind:         RemoveAllowedType,
				Definition:   otherName,
				Name:         relationName,
				AllowedTypes: []AllowedType{subjectType},
			})
		}
	}

	substitutes := slices.DeleteFunc(sortedKeys(vb.working[member.definition].relations), func(relationName string) bool {
		return kind == RemoveRelation && relationName == member.name
	})

	for _, otherName := range sortedKeys(vb.working) {
		other := vb.working[otherName]
		for _, permissionName := range sortedKeys(other.permissions) {
			if (memberTarget{otherName, permissionName}) == member {
				continue
			}

			operation := other.permissions[permissionName]
			use := vb.useOf(otherName, operation, member)
			if !use.direct && !use.arrowTarget {
				continue
			}

			var substitute string
			if use.direct {
				substitute = rapid.SampledFrom(substitutes).Draw(vb.t, vb.label+"substitute:"+otherName+"#"+permissionName)
			}

			rewritten, err := mapOperation(operation, func(leaf schema.Operation) (schema.Operation, error) {
				if relationName, ok := relationReferenceName(leaf); ok {
					if otherName == member.definition && relationName == member.name {
						return schema.NewRelationRef(substitute), nil
					}
					return nil, nil
				}

				left, right, _ := arrowNames(leaf)
				switch {
				case otherName == member.definition && left == member.name:
					return schema.NewRelationRef(substitute), nil
				case right == member.name && vb.reaches(otherName, left, member.definition):
					return schema.NewRelationRef(left), nil
				default:
					return nil, nil
				}
			})
			require.NoError(vb.t, err)

			mutations = append(mutations, Mutation{
				Kind:       ChangePermission,
				Definition: otherName,
				Name:       permissionName,
				Operation:  rewritten,
			})
		}
	}

	return mutations
}

// generateDefinition generates a new definition, with relations and permissions.
func (vb *variantBuilder) generateDefinition() Mutation {
	t := vb.t

	definitionName := rapid.StringMatching("d_"+objectExpr).
		Filter(vb.definitionNameAvailable).
		Draw(t, vb.label+"definitionName")

	// NOTE: the members are generated against a copy of the working schema, so that the
	// permissions can make use of the new relations, and so that the definition can be its
	// own subject type if the schema has no others.
	withDefinition := vb.working.clone()
	withDefinition[definitionName] = &definitionModel{
		relations:   map[string][]AllowedType{},
		permissions: map[string]schema.Operation{},
	}

	relationNames := rapid.SliceOfNDistinct(rapid.StringMatching("r_"+objectExpr), 1, 3, rapid.ID[string]).Draw(t, vb.label+"relationNames")
	permissionNames := rapid.SliceOfNDistinct(rapid.StringMatching("p_"+objectExpr), 0, 2, rapid.ID[string]).Draw(t, vb.label+"permissionNames")

	members := make([]Mutation, 0, len(relationNames)+len(permissionNames))
	for _, relationName := range relationNames {
		member := Mutation{
			Kind:         AddRelation,
			Definition:   definitionName,
			Name:         relationName,
			AllowedTypes: vb.generateAllowedTypes(withDefinition, relationName+"-allowedTypes"),
		}
		require.NoError(t, withDefinition.apply(member))
		members = append(members, member)
	}

	for _, permissionName := range permissionNames {
		members = append(members, Mutation{
			Kind:       AddPermission,
			Definition: definitionName,
			Name:       permissionName,
			Operation:  vb.generateOperation(withDefinition, definitionName, permissionName+"-operation"),
		})
	}

	return Mutation{Kind: AddDefinition, Definition: definitionName, Members: members}
}

// newName generates a name for a new relation or permission in the definition.
func (vb *variantBuilder) newName(prefix string, definitionName string, label string) string {
	return rapid.StringMatching(prefix+objectExpr).
		Filter(func(name string) bool { return vb.nameAvailable(definitionName, name) }).
		Draw(vb.t, vb.label+label)
}

// definitionNameAvailable returns whether the name can be used for a new definition.
func (vb *variantBuilder) definitionNameAvailable(name string) bool {
	// NOTE: a removed definition is no longer in the working schema, but is still targeted.
	_, exists := vb.working[name]
	_, targeted := vb.targetedDefinitions[name]
	return !exists && !targeted
}

// nameAvailable returns whether the name can be used for a new relation or permission in
// the definition.
func (vb *variantBuilder) nameAvailable(definitionName, name string) bool {
	// NOTE: a removed relation or permission is no longer in the working schema, but is
	// still targeted.
	_, targeted := vb.targetedNames[memberTarget{definitionName, name}]
	return !targeted && !vb.working[definitionName].hasName(name)
}

// generateAllowedTypes generates the allowed subject types for a new relation.
func (vb *variantBuilder) generateAllowedTypes(model schemaModel, label string) []AllowedType {
	t := vb.t
	label = vb.label + label

	subjectTypeNames := subjectTypeNamesFor(model)
	chosen := rapid.SliceOfNDistinct(rapid.SampledFrom(subjectTypeNames), 1, min(3, len(subjectTypeNames)), rapid.ID[string]).Draw(t, label)

	allowedTypes := make([]AllowedType, 0, len(chosen)*2)
	for _, subjectTypeName := range chosen {
		subjectRelationNames := sortedKeys(model[subjectTypeName].relations)
		if len(subjectRelationNames) > 0 && rapid.Bool().Draw(t, label+"-"+subjectTypeName+"-withSubjectRelation") {
			allowedTypes = append(allowedTypes, AllowedType{
				Type:        subjectTypeName,
				Subrelation: rapid.SampledFrom(subjectRelationNames).Draw(t, label+"-"+subjectTypeName+"-subjectRelation"),
			})
		}

		allowedTypes = append(allowedTypes, AllowedType{Type: subjectTypeName, Subrelation: tuple.Ellipsis})
	}
	return allowedTypes
}

// generateOperation generates an expression for a permission in the definition.
func (vb *variantBuilder) generateOperation(model schemaModel, definitionName string, label string) schema.Operation {
	return generateOperationFor(vb.t, model, definitionName, 3, vb.label+label)
}

// generateOperationFor generates an expression for a permission in the definition, nested
// to at most the given depth.
func generateOperationFor(t *rapid.T, model schemaModel, definitionName string, depth int, label string) schema.Operation {
	definition := model[definitionName]

	// An arrow target must exist on every type allowed by its left relation.
	var arrowChoices []arrowChoice
	for _, relationName := range sortedKeys(definition.relations) {
		var targets *mapz.Set[string]
		for _, allowedType := range definition.relations[relationName] {
			found := mapz.NewSet(sortedKeys(model[allowedType.Type].relations)...)
			if targets == nil {
				targets = found
			} else {
				targets = targets.Intersect(found)
			}
		}

		if targets == nil {
			continue
		}

		sortedTargets := targets.AsSlice()
		slices.Sort(sortedTargets)
		for _, target := range sortedTargets {
			arrowChoices = append(arrowChoices, arrowChoice{relationName, target})
		}
	}

	return mustGenerateOperation(t, sortedKeys(definition.relations), arrowChoices, depth, label)
}

// subjectTypeNamesFor returns the names of the definitions to use as subject types in new
// relations: those already used as subject types, or all definitions if there are none.
func subjectTypeNamesFor(model schemaModel) []string {
	subjectTypeNames := mapz.NewSet[string]()
	for _, definition := range model {
		for _, allowedTypes := range definition.relations {
			for _, allowedType := range allowedTypes {
				subjectTypeNames.Add(allowedType.Type)
			}
		}
	}

	if subjectTypeNames.IsEmpty() {
		return sortedKeys(model)
	}

	sorted := subjectTypeNames.AsSlice()
	slices.Sort(sorted)
	return sorted
}

// operationsEquivalent returns whether the two operations compile to the same expression,
// which is what a schema diff compares. As a schema is typically written as text, operations
// are also considered equivalent if they compile to the same expression once written as
// schema text: `viewer` and an intersection consisting solely of `viewer`, for example.
func operationsEquivalent(first, second schema.Operation) bool {
	for _, viaText := range []bool{false, true} {
		firstRewrite, err := compileOperation(first, viaText)
		if err != nil {
			return false
		}

		secondRewrite, err := compileOperation(second, viaText)
		if err != nil {
			return false
		}

		if proto.Equal(firstRewrite, secondRewrite) {
			return true
		}
	}
	return false
}

// operationsIdentical returns whether the two operations have the same structure. Unlike
// operationsEquivalent, `viewer` and an intersection consisting solely of `viewer` are not
// identical.
func operationsIdentical(first, second schema.Operation) bool {
	firstRewrite, err := compileOperation(first, false)
	if err != nil {
		return false
	}

	secondRewrite, err := compileOperation(second, false)
	if err != nil {
		return false
	}

	return proto.Equal(firstRewrite, secondRewrite)
}

// compileOperation returns the compiled form of the operation, optionally compiling it by
// way of schema text.
func compileOperation(op schema.Operation, viaText bool) (*core.UsersetRewrite, error) {
	copied, err := copyOperation(op)
	if err != nil {
		return nil, err
	}

	built := schema.NewSchemaBuilder().
		AddDefinition("resource").
		AddPermission("target").
		Operation(copied).
		Done().
		Done().
		Build()

	definitions, _, err := built.ToDefinitions()
	if err != nil {
		return nil, err
	}

	if viaText {
		schemaText, _, err := generator.GenerateSchema(context.Background(), []compiler.SchemaDefinition{definitions[0]})
		if err != nil {
			return nil, err
		}

		compiled, err := compiler.Compile(
			compiler.InputSchema{Source: input.Source("operation"), SchemaString: schemaText},
			compiler.AllowUnprefixedObjectType(),
		)
		if err != nil {
			return nil, err
		}

		// NOTE: converting by way of a schema removes the source positions.
		recompiled, err := schema.BuildSchemaFromCompiledSchema(*compiled)
		if err != nil {
			return nil, err
		}

		definitions, _, err = recompiled.ToDefinitions()
		if err != nil {
			return nil, err
		}
	}

	if len(definitions) != 1 || len(definitions[0].Relation) != 1 {
		return nil, errors.New("unexpected compiled form for operation")
	}
	return definitions[0].Relation[0].UsersetRewrite, nil
}

// relationshipShape is a kind of relationship that a schema allows.
type relationshipShape struct {
	definition string
	relation   string
	subject    AllowedType
}

// shapesOnlyIn returns the kinds of relationships allowed by the first schema and not by
// the second, in a stable order.
func shapesOnlyIn(first, second schemaModel) []relationshipShape {
	var shapes []relationshipShape
	for _, definitionName := range sortedKeys(first) {
		definition := first[definitionName]
		for _, relationName := range sortedKeys(definition.relations) {
			for _, allowedType := range definition.relations[relationName] {
				if !second.allows(definitionName, relationName, allowedType) {
					shapes = append(shapes, relationshipShape{definitionName, relationName, allowedType})
				}
			}
		}
	}
	return shapes
}

// generateRelationshipsFor generates an infinite sequence of relationships of the given
// shapes, or an empty sequence if there are none.
func generateRelationshipsFor(t *rapid.T, shapes []relationshipShape, objectIDs []string, label string) iter.Seq[tuple.Relationship] {
	rapidObjectString := objectIDGenerator(objectIDs)

	return func(yield func(tuple.Relationship) bool) {
		if len(shapes) == 0 {
			return
		}

		for {
			shapeIndex := rapid.IntRange(0, len(shapes)-1).Draw(t, label+"Shape")
			shape := shapes[shapeIndex]

			relationship := tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: shape.definition,
						ObjectID:   rapidObjectString.Draw(t, label+"ResourceID"),
						Relation:   shape.relation,
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: shape.subject.Type,
						ObjectID:   rapidObjectString.Draw(t, label+"SubjectID"),
						Relation:   shape.subject.Subrelation,
					},
				},
			}

			if !yield(relationship) {
				return
			}
		}
	}
}

// GenerateRemovedRelationships generates an infinite sequence of relationships that are valid
// under Base and are not valid under Schema. The sequence is empty if no mutation breaks data.
func (v Variant) GenerateRemovedRelationships(t *rapid.T) iter.Seq[tuple.Relationship] {
	return generateRelationshipsFor(t, shapesOnlyIn(v.baseModel, v.model), v.objectIDs, "removed")
}

// GenerateAddedRelationships generates an infinite sequence of relationships that are valid
// under Schema and are not valid under Base. The sequence is empty if no mutation allows for
// new data.
func (v Variant) GenerateAddedRelationships(t *rapid.T) iter.Seq[tuple.Relationship] {
	return generateRelationshipsFor(t, shapesOnlyIn(v.model, v.baseModel), v.objectIDs, "added")
}
