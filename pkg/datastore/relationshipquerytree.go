package datastore

type RelationshipQueryOperation int

const (
	RelationshipQueryNone RelationshipQueryOperation = 0
	RelationshipQueryOr   RelationshipQueryOperation = 1
	RelationshipQueryAnd  RelationshipQueryOperation = 2
)

type RelationshipsQueryTree struct {
	op       RelationshipQueryOperation
	filter   RelationshipsFilter
	children []RelationshipsQueryTree
}

func NewRelationshipQueryTree(filter RelationshipsFilter) RelationshipsQueryTree {
	return RelationshipsQueryTree{
		op:       RelationshipQueryNone,
		filter:   filter,
		children: nil,
	}
}
