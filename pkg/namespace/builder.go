package namespace

import (
	"github.com/authzed/spicedb/pkg/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Namespace creates a namespace definition with one or more defined relations.
func Namespace(name string, relations ...*core.Relation) *core.NamespaceDefinition {
	return &core.NamespaceDefinition{
		Name:     name,
		Relation: relations,
	}
}

// WithComment creates a namespace definition with one or more defined relations.
func WithComment(name string, comment string, relations ...*core.Relation) *core.NamespaceDefinition {
	nd := Namespace(name, relations...)
	nd.Metadata, _ = AddComment(nd.Metadata, comment)
	return nd
}

// MustRelation creates a relation definition with an optional rewrite definition.
func MustRelation(name string, rewrite *core.UsersetRewrite, allowedDirectRelations ...*core.AllowedRelation) *core.Relation {
	r, err := Relation(name, rewrite, allowedDirectRelations...)
	if err != nil {
		panic(err)
	}
	return r
}

// Relation creates a relation definition with an optional rewrite definition.
func Relation(name string, rewrite *core.UsersetRewrite, allowedDirectRelations ...*core.AllowedRelation) (*core.Relation, error) {
	var typeInfo *core.TypeInformation
	if len(allowedDirectRelations) > 0 {
		typeInfo = &core.TypeInformation{
			AllowedDirectRelations: allowedDirectRelations,
		}
	}

	rel := &core.Relation{
		Name:            name,
		UsersetRewrite:  rewrite,
		TypeInformation: typeInfo,
	}

	switch {
	case rewrite != nil && len(allowedDirectRelations) == 0:
		if err := SetRelationKind(rel, iv1.RelationMetadata_PERMISSION); err != nil {
			return nil, spiceerrors.MustBugf("failed to set relation kind: %s", err.Error())
		}

	case rewrite == nil && len(allowedDirectRelations) > 0:
		if err := SetRelationKind(rel, iv1.RelationMetadata_RELATION); err != nil {
			return nil, spiceerrors.MustBugf("failed to set relation kind: %s", err.Error())
		}

	default:
		// By default we do not set a relation kind on the relation. Relations without any
		// information, or relations with both rewrites and types are legacy relations from
		// before the DSL schema and, as such, do not have a defined "kind".
	}

	return rel, nil
}

// MustRelationWithComment creates a relation definition with an optional rewrite definition.
func MustRelationWithComment(name string, comment string, rewrite *core.UsersetRewrite, allowedDirectRelations ...*core.AllowedRelation) *core.Relation {
	rel := MustRelation(name, rewrite, allowedDirectRelations...)
	rel.Metadata, _ = AddComment(rel.Metadata, comment)
	return rel
}

// AllowedRelation creates a relation reference to an allowed relation.
func AllowedRelation(namespaceName string, relationName string) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &core.AllowedRelation_Relation{
			Relation: relationName,
		},
	}
}

// AllowedRelationWithCaveat creates a relation reference to an allowed relation.
func AllowedRelationWithCaveat(namespaceName string, relationName string, withCaveat *core.AllowedCaveat) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &core.AllowedRelation_Relation{
			Relation: relationName,
		},
		RequiredCaveat: withCaveat,
	}
}

// WithExpiration adds the expiration trait to the allowed relation.
func WithExpiration(allowedRelation *core.AllowedRelation) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace:          allowedRelation.Namespace,
		RelationOrWildcard: allowedRelation.RelationOrWildcard,
		RequiredCaveat:     allowedRelation.RequiredCaveat,
		RequiredExpiration: &core.ExpirationTrait{},
	}
}

// AllowedRelationWithExpiration creates a relation reference to an allowed relation.
func AllowedRelationWithExpiration(namespaceName string, relationName string) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &core.AllowedRelation_Relation{
			Relation: relationName,
		},
		RequiredExpiration: &core.ExpirationTrait{},
	}
}

// AllowedRelationWithCaveatAndExpiration creates a relation reference to an allowed relation.
func AllowedRelationWithCaveatAndExpiration(namespaceName string, relationName string, withCaveat *core.AllowedCaveat) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &core.AllowedRelation_Relation{
			Relation: relationName,
		},
		RequiredExpiration: &core.ExpirationTrait{},
		RequiredCaveat:     withCaveat,
	}
}

// AllowedPublicNamespace creates a relation reference to an allowed public namespace.
func AllowedPublicNamespace(namespaceName string) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &core.AllowedRelation_PublicWildcard_{
			PublicWildcard: &core.AllowedRelation_PublicWildcard{},
		},
	}
}

// AllowedCaveat creates a caveat reference.
func AllowedCaveat(name string) *core.AllowedCaveat {
	return &core.AllowedCaveat{
		CaveatName: name,
	}
}

// CaveatDefinition returns a new caveat definition.
func CaveatDefinition(env *caveats.Environment, name string, expr string) (*core.CaveatDefinition, error) {
	compiled, err := caveats.CompileCaveatWithName(env, expr, name)
	if err != nil {
		return nil, err
	}
	return CompiledCaveatDefinition(env, name, compiled)
}

// CompiledCaveatDefinition returns a new caveat definition.
func CompiledCaveatDefinition(env *caveats.Environment, name string, compiled *caveats.CompiledCaveat) (*core.CaveatDefinition, error) {
	if compiled == nil {
		return nil, spiceerrors.MustBugf("compiled caveat is nil")
	}

	serialized, err := compiled.Serialize()
	if err != nil {
		return nil, err
	}
	return &core.CaveatDefinition{
		Name:                 name,
		SerializedExpression: serialized,
		ParameterTypes:       env.EncodedParametersTypes(),
	}, nil
}

// MustCaveatDefinition returns a new caveat definition.
func MustCaveatDefinition(env *caveats.Environment, name string, expr string) *core.CaveatDefinition {
	cd, err := CaveatDefinition(env, name, expr)
	if err != nil {
		panic(err)
	}
	return cd
}

// MustCaveatDefinitionWithComment returns a new caveat definition.
func MustCaveatDefinitionWithComment(env *caveats.Environment, name string, comment string, expr string) *core.CaveatDefinition {
	cd, err := CaveatDefinition(env, name, expr)
	if err != nil {
		panic(err)
	}
	cd.Metadata, _ = AddComment(cd.Metadata, comment)
	return cd
}

// AllowedPublicNamespaceWithCaveat creates a relation reference to an allowed public namespace.
func AllowedPublicNamespaceWithCaveat(namespaceName string, withCaveat *core.AllowedCaveat) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &core.AllowedRelation_PublicWildcard_{
			PublicWildcard: &core.AllowedRelation_PublicWildcard{},
		},
		RequiredCaveat: withCaveat,
	}
}

// RelationReference creates a relation reference.
func RelationReference(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

// Union creates a rewrite definition that combines/considers usersets in all children.
func Union(firstChild *core.SetOperation_Child, rest ...*core.SetOperation_Child) *core.UsersetRewrite {
	return &core.UsersetRewrite{
		RewriteOperation: &core.UsersetRewrite_Union{
			Union: setOperation(firstChild, rest),
		},
	}
}

// Intersection creates a rewrite definition that returns/considers only usersets present in all children.
func Intersection(firstChild *core.SetOperation_Child, rest ...*core.SetOperation_Child) *core.UsersetRewrite {
	return &core.UsersetRewrite{
		RewriteOperation: &core.UsersetRewrite_Intersection{
			Intersection: setOperation(firstChild, rest),
		},
	}
}

// Exclusion creates a rewrite definition that starts with the usersets of the first child
// and iteratively removes usersets that appear in the remaining children.
func Exclusion(firstChild *core.SetOperation_Child, rest ...*core.SetOperation_Child) *core.UsersetRewrite {
	return &core.UsersetRewrite{
		RewriteOperation: &core.UsersetRewrite_Exclusion{
			Exclusion: setOperation(firstChild, rest),
		},
	}
}

func setOperation(firstChild *core.SetOperation_Child, rest []*core.SetOperation_Child) *core.SetOperation {
	children := append([]*core.SetOperation_Child{firstChild}, rest...)
	return &core.SetOperation{
		Child: children,
	}
}

// Nil creates a child for a set operation that references the empty set.
func Nil() *core.SetOperation_Child {
	return &core.SetOperation_Child{
		ChildType: &core.SetOperation_Child_XNil{},
	}
}

// ComputesUserset creates a child for a set operation that follows a relation on the given starting object.
func ComputedUserset(relation string) *core.SetOperation_Child {
	return &core.SetOperation_Child{
		ChildType: &core.SetOperation_Child_ComputedUserset{
			ComputedUserset: &core.ComputedUserset{
				Relation: relation,
			},
		},
	}
}

// MustComputesUsersetWithSourcePosition creates a child for a set operation that follows a relation on the given starting object.
func MustComputesUsersetWithSourcePosition(relation string, lineNumber uint64) *core.SetOperation_Child {
	cu := &core.ComputedUserset{
		Relation: relation,
	}
	cu.SourcePosition = &core.SourcePosition{
		ZeroIndexedLineNumber:     lineNumber,
		ZeroIndexedColumnPosition: 0,
	}

	return &core.SetOperation_Child{
		ChildType: &core.SetOperation_Child_ComputedUserset{
			ComputedUserset: cu,
		},
	}
}

// TupleToUserset creates a child which first loads all tuples with the specific relation,
// and then unions all children on the usersets found by following a relation on those loaded
// tuples.
func TupleToUserset(tuplesetRelation, usersetRelation string) *core.SetOperation_Child {
	return &core.SetOperation_Child{
		ChildType: &core.SetOperation_Child_TupleToUserset{
			TupleToUserset: &core.TupleToUserset{
				Tupleset: &core.TupleToUserset_Tupleset{
					Relation: tuplesetRelation,
				},
				ComputedUserset: &core.ComputedUserset{
					Relation: usersetRelation,
					Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
				},
			},
		},
	}
}

// MustFunctionedTupleToUserset creates a child which first loads all tuples with the specific relation,
// and then applies the function to all children on the usersets found by following a relation on those loaded
// tuples.
func MustFunctionedTupleToUserset(tuplesetRelation, functionName, usersetRelation string) *core.SetOperation_Child {
	function := core.FunctionedTupleToUserset_FUNCTION_ANY

	switch functionName {
	case "any":
		// already set to any

	case "all":
		function = core.FunctionedTupleToUserset_FUNCTION_ALL

	default:
		panic(spiceerrors.MustBugf("unknown function name: %s", functionName))
	}

	return &core.SetOperation_Child{
		ChildType: &core.SetOperation_Child_FunctionedTupleToUserset{
			FunctionedTupleToUserset: &core.FunctionedTupleToUserset{
				Function: function,
				Tupleset: &core.FunctionedTupleToUserset_Tupleset{
					Relation: tuplesetRelation,
				},
				ComputedUserset: &core.ComputedUserset{
					Relation: usersetRelation,
					Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
				},
			},
		},
	}
}

// Rewrite wraps a rewrite as a set operation child of another rewrite.
func Rewrite(rewrite *core.UsersetRewrite) *core.SetOperation_Child {
	return &core.SetOperation_Child{
		ChildType: &core.SetOperation_Child_UsersetRewrite{
			UsersetRewrite: rewrite,
		},
	}
}
