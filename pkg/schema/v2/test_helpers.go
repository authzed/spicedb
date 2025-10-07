package schema

import "github.com/authzed/spicedb/pkg/tuple"

// NewTestBaseRelation creates a BaseRelation for testing with the proper parent structure.
// This is exported for use by external test packages that cannot directly create
// struct literals with unexported fields.
func NewTestBaseRelation(defName, relationName, subjectType, subrelation string) *BaseRelation {
	// Create the schema hierarchy: Schema -> Definition -> Relation -> BaseRelation
	testSchema := &Schema{
		definitions: make(map[string]*Definition),
	}

	testDefinition := &Definition{
		parent:    testSchema,
		name:      defName,
		relations: make(map[string]*Relation),
	}
	testSchema.definitions[defName] = testDefinition

	testRelation := &Relation{
		parent: testDefinition,
		name:   relationName,
	}
	testDefinition.relations[relationName] = testRelation

	return &BaseRelation{
		parent:      testRelation,
		subjectType: subjectType,
		subrelation: subrelation,
		caveat:      "",
		expiration:  false,
	}
}

// NewTestBaseRelationWithFeatures creates a BaseRelation with caveat and expiration features.
func NewTestBaseRelationWithFeatures(defName, relationName, subjectType, subrelation, caveat string, expiration bool) *BaseRelation {
	baseRel := NewTestBaseRelation(defName, relationName, subjectType, subrelation)
	baseRel.caveat = caveat
	baseRel.expiration = expiration
	return baseRel
}

// NewTestWildcardBaseRelation creates a BaseRelation for wildcard testing.
func NewTestWildcardBaseRelation(defName, relationName, subjectType string) *BaseRelation {
	baseRel := NewTestBaseRelation(defName, relationName, subjectType, tuple.Ellipsis)
	baseRel.wildcard = true
	return baseRel
}

// NewTestWildcardBaseRelationWithFeatures creates a wildcard BaseRelation with caveat and expiration features.
func NewTestWildcardBaseRelationWithFeatures(defName, relationName, subjectType, caveat string, expiration bool) *BaseRelation {
	baseRel := NewTestWildcardBaseRelation(defName, relationName, subjectType)
	baseRel.caveat = caveat
	baseRel.expiration = expiration
	return baseRel
}
