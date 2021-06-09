package namespace

import (
	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
)

// Namespace creates a namespace definition with one or more defined relations.
func Namespace(name string, relations ...*pb.Relation) *pb.NamespaceDefinition {
	return &pb.NamespaceDefinition{
		Name:     name,
		Relation: relations,
	}
}

// Relation creates a relation definition with an optional rewrite definition.
func Relation(name string, rewrite *pb.UsersetRewrite, allowedDirectRelations ...*pb.RelationReference) *pb.Relation {
	var typeInfo *pb.TypeInformation
	if len(allowedDirectRelations) > 0 {
		typeInfo = &pb.TypeInformation{
			AllowedDirectRelations: allowedDirectRelations,
		}
	}

	return &pb.Relation{
		Name:            name,
		UsersetRewrite:  rewrite,
		TypeInformation: typeInfo,
	}
}

// RelationReference creates a relation reference.
func RelationReference(namespaceName string, relationName string) *pb.RelationReference {
	return &pb.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

// Union creates a rewrite definition that combines/considers usersets in all children.
func Union(firstChild *pb.SetOperation_Child, rest ...*pb.SetOperation_Child) *pb.UsersetRewrite {
	return &pb.UsersetRewrite{
		RewriteOperation: &pb.UsersetRewrite_Union{
			Union: setOperation(firstChild, rest),
		},
	}
}

// Intersection creates a rewrite definition that returns/considers only usersets present in all children.
func Intersection(firstChild *pb.SetOperation_Child, rest ...*pb.SetOperation_Child) *pb.UsersetRewrite {
	return &pb.UsersetRewrite{
		RewriteOperation: &pb.UsersetRewrite_Intersection{
			Intersection: setOperation(firstChild, rest),
		},
	}
}

// Exclusion creates a rewrite definition that starts with the usersets of the first child
// and iteratively removes usersets that appear in the remaining children.
func Exclusion(firstChild *pb.SetOperation_Child, rest ...*pb.SetOperation_Child) *pb.UsersetRewrite {
	return &pb.UsersetRewrite{
		RewriteOperation: &pb.UsersetRewrite_Exclusion{
			Exclusion: setOperation(firstChild, rest),
		},
	}
}

func setOperation(firstChild *pb.SetOperation_Child, rest []*pb.SetOperation_Child) *pb.SetOperation {
	children := append([]*pb.SetOperation_Child{firstChild}, rest...)
	return &pb.SetOperation{
		Child: children,
	}
}

// This creates a child for a set operation that references only direct usersets with the parent relation.
func This() *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_XThis{},
	}
}

// ComputesUserset creates a child for a set operation that follows a relation on the given starting object.
func ComputedUserset(relation string) *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_ComputedUserset{
			ComputedUserset: &pb.ComputedUserset{
				Relation: relation,
			},
		},
	}
}

// TupleToUserset creates a child which first loads all tuples with the specific relation,
// and then unions all children on the usersets found by following a relation on those loaded
// tuples.
func TupleToUserset(tuplesetRelation, usersetRelation string) *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_TupleToUserset{
			TupleToUserset: &pb.TupleToUserset{
				Tupleset: &pb.TupleToUserset_Tupleset{
					Relation: tuplesetRelation,
				},
				ComputedUserset: &pb.ComputedUserset{
					Relation: usersetRelation,
					Object:   pb.ComputedUserset_TUPLE_USERSET_OBJECT,
				},
			},
		},
	}
}

// Rewrite wraps a rewrite as a set operation child of another rewrite.
func Rewrite(rewrite *pb.UsersetRewrite) *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_UsersetRewrite{
			UsersetRewrite: rewrite,
		},
	}
}
