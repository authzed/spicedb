package namespace

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	iv1 "github.com/authzed/spicedb/internal/proto/impl/v1"
)

// Namespace creates a namespace definition with one or more defined relations.
func Namespace(name string, relations ...*v0.Relation) *v0.NamespaceDefinition {
	return &v0.NamespaceDefinition{
		Name:     name,
		Relation: relations,
	}
}

// NamespaceWithComment creates a namespace definition with one or more defined relations.
func NamespaceWithComment(name string, comment string, relations ...*v0.Relation) *v0.NamespaceDefinition {
	nd := Namespace(name, relations...)
	nd.Metadata, _ = AddComment(nd.Metadata, comment)
	return nd
}

// Relation creates a relation definition with an optional rewrite definition.
func Relation(name string, rewrite *v0.UsersetRewrite, allowedDirectRelations ...*v0.AllowedRelation) *v0.Relation {
	var typeInfo *v0.TypeInformation
	if len(allowedDirectRelations) > 0 {
		typeInfo = &v0.TypeInformation{
			AllowedDirectRelations: allowedDirectRelations,
		}
	}

	rel := &v0.Relation{
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
func RelationWithComment(name string, comment string, rewrite *v0.UsersetRewrite, allowedDirectRelations ...*v0.AllowedRelation) *v0.Relation {
	rel := Relation(name, rewrite, allowedDirectRelations...)
	rel.Metadata, _ = AddComment(rel.Metadata, comment)
	return rel
}

// AllowedRelation creates a relation reference to an allowed relation.
func AllowedRelation(namespaceName string, relationName string) *v0.AllowedRelation {
	return &v0.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &v0.AllowedRelation_Relation{
			Relation: relationName,
		},
	}
}

// AllowedPublicNamespace creates a relation reference to an allowed public namespace.
func AllowedPublicNamespace(namespaceName string) *v0.AllowedRelation {
	return &v0.AllowedRelation{
		Namespace: namespaceName,
		RelationOrWildcard: &v0.AllowedRelation_PublicWildcard_{
			PublicWildcard: &v0.AllowedRelation_PublicWildcard{},
		},
	}
}

// RelationReference creates a relation reference.
func RelationReference(namespaceName string, relationName string) *v0.RelationReference {
	return &v0.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

// Union creates a rewrite definition that combines/considers usersets in all children.
func Union(firstChild *v0.SetOperation_Child, rest ...*v0.SetOperation_Child) *v0.UsersetRewrite {
	return &v0.UsersetRewrite{
		RewriteOperation: &v0.UsersetRewrite_Union{
			Union: setOperation(firstChild, rest),
		},
	}
}

// Intersection creates a rewrite definition that returns/considers only usersets present in all children.
func Intersection(firstChild *v0.SetOperation_Child, rest ...*v0.SetOperation_Child) *v0.UsersetRewrite {
	return &v0.UsersetRewrite{
		RewriteOperation: &v0.UsersetRewrite_Intersection{
			Intersection: setOperation(firstChild, rest),
		},
	}
}

// Exclusion creates a rewrite definition that starts with the usersets of the first child
// and iteratively removes usersets that appear in the remaining children.
func Exclusion(firstChild *v0.SetOperation_Child, rest ...*v0.SetOperation_Child) *v0.UsersetRewrite {
	return &v0.UsersetRewrite{
		RewriteOperation: &v0.UsersetRewrite_Exclusion{
			Exclusion: setOperation(firstChild, rest),
		},
	}
}

func setOperation(firstChild *v0.SetOperation_Child, rest []*v0.SetOperation_Child) *v0.SetOperation {
	children := append([]*v0.SetOperation_Child{firstChild}, rest...)
	return &v0.SetOperation{
		Child: children,
	}
}

// This creates a child for a set operation that references only direct usersets with the parent relation.
func This() *v0.SetOperation_Child {
	return &v0.SetOperation_Child{
		ChildType: &v0.SetOperation_Child_XThis{},
	}
}

// ComputesUserset creates a child for a set operation that follows a relation on the given starting object.
func ComputedUserset(relation string) *v0.SetOperation_Child {
	return &v0.SetOperation_Child{
		ChildType: &v0.SetOperation_Child_ComputedUserset{
			ComputedUserset: &v0.ComputedUserset{
				Relation: relation,
			},
		},
	}
}

// TupleToUserset creates a child which first loads all tuples with the specific relation,
// and then unions all children on the usersets found by following a relation on those loaded
// tuples.
func TupleToUserset(tuplesetRelation, usersetRelation string) *v0.SetOperation_Child {
	return &v0.SetOperation_Child{
		ChildType: &v0.SetOperation_Child_TupleToUserset{
			TupleToUserset: &v0.TupleToUserset{
				Tupleset: &v0.TupleToUserset_Tupleset{
					Relation: tuplesetRelation,
				},
				ComputedUserset: &v0.ComputedUserset{
					Relation: usersetRelation,
					Object:   v0.ComputedUserset_TUPLE_USERSET_OBJECT,
				},
			},
		},
	}
}

// Rewrite wraps a rewrite as a set operation child of another rewrite.
func Rewrite(rewrite *v0.UsersetRewrite) *v0.SetOperation_Child {
	return &v0.SetOperation_Child{
		ChildType: &v0.SetOperation_Child_UsersetRewrite{
			UsersetRewrite: rewrite,
		},
	}
}
