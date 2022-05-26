package namespace

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
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

// Relation creates a relation definition with an optional rewrite definition.
func Relation(name string, rewrite *core.UsersetRewrite, allowedDirectRelations ...*core.AllowedRelation) *core.Relation {
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
			panic("failed to set relation kind: " + err.Error())
		}

	case rewrite == nil && len(allowedDirectRelations) > 0:
		if err := SetRelationKind(rel, iv1.RelationMetadata_RELATION); err != nil {
			panic("failed to set relation kind: " + err.Error())
		}

	default:
		// By default we do not set a relation kind on the relation. Relations without any
		// information, or relations with both rewrites and types are legacy relations from
		// before the DSL schema and, as such, do not have a defined "kind".
	}

	return rel
}

// RelationWithComment creates a relation definition with an optional rewrite definition.
func RelationWithComment(name string, comment string, rewrite *core.UsersetRewrite, allowedDirectRelations ...*core.AllowedRelation) *core.Relation {
	rel := Relation(name, rewrite, allowedDirectRelations...)
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

// AllowedPublicNamespace creates a relation reference to an allowed public namespace.
func AllowedPublicNamespace(namespaceName string) *core.AllowedRelation {
	return &core.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &core.AllowedRelation_PublicWildcard_{
			PublicWildcard: &core.AllowedRelation_PublicWildcard{},
		},
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

// Rewrite wraps a rewrite as a set operation child of another rewrite.
func Rewrite(rewrite *core.UsersetRewrite) *core.SetOperation_Child {
	return &core.SetOperation_Child{
		ChildType: &core.SetOperation_Child_UsersetRewrite{
			UsersetRewrite: rewrite,
		},
	}
}
