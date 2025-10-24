package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

var errTestError = errors.New("test error")

// testVisitor implements all visitor interfaces to track what was visited
type testVisitor struct {
	schemas            []*Schema
	definitions        []*Definition
	caveats            []*Caveat
	relations          []*Relation
	baseRelations      []*BaseRelation
	permissions        []*Permission
	operations         []Operation
	relationReferences []*RelationReference
	nilReferences      []*NilReference
	arrowReferences    []*ArrowReference
	unionOperations    []*UnionOperation
	intersectionOps    []*IntersectionOperation
	exclusionOps       []*ExclusionOperation
}

func (tv *testVisitor) VisitSchema(s *Schema, value struct{}) (struct{}, bool, error) {
	tv.schemas = append(tv.schemas, s)
	return value, true, nil
}

func (tv *testVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	tv.definitions = append(tv.definitions, d)
	return value, true, nil
}

func (tv *testVisitor) VisitCaveat(c *Caveat, value struct{}) (struct{}, error) {
	tv.caveats = append(tv.caveats, c)
	return value, nil
}

func (tv *testVisitor) VisitRelation(r *Relation, value struct{}) (struct{}, bool, error) {
	tv.relations = append(tv.relations, r)
	return value, true, nil
}

func (tv *testVisitor) VisitBaseRelation(br *BaseRelation, value struct{}) (struct{}, error) {
	tv.baseRelations = append(tv.baseRelations, br)
	return value, nil
}

func (tv *testVisitor) VisitPermission(p *Permission, value struct{}) (struct{}, bool, error) {
	tv.permissions = append(tv.permissions, p)
	return value, true, nil
}

func (tv *testVisitor) VisitOperation(op Operation, value struct{}) (struct{}, bool, error) {
	tv.operations = append(tv.operations, op)
	return value, true, nil
}

func (tv *testVisitor) VisitRelationReference(rr *RelationReference, value struct{}) (struct{}, error) {
	tv.relationReferences = append(tv.relationReferences, rr)
	return value, nil
}

func (tv *testVisitor) VisitNilReference(nr *NilReference, value struct{}) (struct{}, error) {
	tv.nilReferences = append(tv.nilReferences, nr)
	return value, nil
}

func (tv *testVisitor) VisitArrowReference(ar *ArrowReference, value struct{}) (struct{}, error) {
	tv.arrowReferences = append(tv.arrowReferences, ar)
	return value, nil
}

func (tv *testVisitor) VisitUnionOperation(uo *UnionOperation, value struct{}) (struct{}, bool, error) {
	tv.unionOperations = append(tv.unionOperations, uo)
	return value, true, nil
}

func (tv *testVisitor) VisitIntersectionOperation(io *IntersectionOperation, value struct{}) (struct{}, bool, error) {
	tv.intersectionOps = append(tv.intersectionOps, io)
	return value, true, nil
}

func (tv *testVisitor) VisitExclusionOperation(eo *ExclusionOperation, value struct{}) (struct{}, bool, error) {
	tv.exclusionOps = append(tv.exclusionOps, eo)
	return value, true, nil
}

func TestWalkSchema_Nil(t *testing.T) {
	visitor := &testVisitor{}
	_, err := WalkSchema(nil, visitor, struct{}{})
	require.NoError(t, err)

	require.Empty(t, visitor.schemas)
	require.Empty(t, visitor.definitions)
}

func TestWalkSchema_Empty(t *testing.T) {
	schema := &Schema{
		definitions: make(map[string]*Definition),
		caveats:     make(map[string]*Caveat),
	}

	visitor := &testVisitor{}
	_, err := WalkSchema(schema, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.schemas, 1)
	require.Same(t, schema, visitor.schemas[0])
	require.Empty(t, visitor.definitions)
	require.Empty(t, visitor.caveats)
}

func TestWalkSchema_WithDefinitionsAndCaveats(t *testing.T) {
	schema := &Schema{
		definitions: map[string]*Definition{
			"user": {
				name:        "user",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
			"document": {
				name:        "document",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
		},
		caveats: map[string]*Caveat{
			"is_admin": {
				name:       "is_admin",
				expression: "admin == true",
			},
		},
	}
	schema.definitions["user"].parent = schema
	schema.definitions["document"].parent = schema
	schema.caveats["is_admin"].parent = schema

	visitor := &testVisitor{}
	_, err := WalkSchema(schema, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.schemas, 1)
	require.Len(t, visitor.definitions, 2)
	require.Len(t, visitor.caveats, 1)
}

func TestWalkDefinition_WithRelations(t *testing.T) {
	rel1 := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}
	rel2 := &Relation{
		name:          "editor",
		baseRelations: []*BaseRelation{},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel1,
			"editor": rel2,
		},
		permissions: make(map[string]*Permission),
	}
	rel1.parent = def
	rel2.parent = def

	visitor := &testVisitor{}
	_, err := WalkDefinition(def, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.definitions, 1)
	require.Len(t, visitor.relations, 2)
}

func TestWalkRelation_WithBaseRelations(t *testing.T) {
	br1 := &BaseRelation{
		subjectType: "user",
	}
	br2 := &BaseRelation{
		subjectType: "user",
		subrelation: "viewer",
	}
	br3 := &BaseRelation{
		subjectType: "user",
		wildcard:    true,
	}

	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{br1, br2, br3},
	}
	br1.parent = rel
	br2.parent = rel
	br3.parent = rel

	visitor := &testVisitor{}
	_, err := WalkRelation(rel, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.relations, 1)
	require.Len(t, visitor.baseRelations, 3)
}

func TestWalkPermission_WithSimpleOperation(t *testing.T) {
	op := &RelationReference{
		relationName: "viewer",
	}

	perm := &Permission{
		name:      "view",
		operation: op,
	}

	visitor := &testVisitor{}
	_, err := WalkPermission(perm, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.permissions, 1)
	require.Len(t, visitor.operations, 1)
	require.Len(t, visitor.relationReferences, 1)
	require.Equal(t, "viewer", visitor.relationReferences[0].RelationName())
}

func TestWalkOperation_RelationReference(t *testing.T) {
	op := &RelationReference{
		relationName: "viewer",
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 1)
	require.Len(t, visitor.relationReferences, 1)
	require.Equal(t, "viewer", visitor.relationReferences[0].RelationName())
}

func TestWalkOperation_NilReference(t *testing.T) {
	op := &NilReference{}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 1)
	require.Len(t, visitor.nilReferences, 1)
	require.NotNil(t, visitor.nilReferences[0])
}

func TestWalkOperation_ArrowReference(t *testing.T) {
	op := &ArrowReference{
		left:  "parent",
		right: "viewer",
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 1)
	require.Len(t, visitor.arrowReferences, 1)
	require.Equal(t, "parent", visitor.arrowReferences[0].Left())
	require.Equal(t, "viewer", visitor.arrowReferences[0].Right())
}

func TestWalkOperation_UnionOperation(t *testing.T) {
	op := &UnionOperation{
		children: []Operation{
			&RelationReference{relationName: "viewer"},
			&RelationReference{relationName: "editor"},
			&RelationReference{relationName: "admin"},
		},
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 4) // 1 union + 3 children
	require.Len(t, visitor.unionOperations, 1)
	require.Len(t, visitor.relationReferences, 3)
}

func TestWalkOperation_IntersectionOperation(t *testing.T) {
	op := &IntersectionOperation{
		children: []Operation{
			&RelationReference{relationName: "viewer"},
			&RelationReference{relationName: "approved"},
		},
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 3) // 1 intersection + 2 children
	require.Len(t, visitor.intersectionOps, 1)
	require.Len(t, visitor.relationReferences, 2)
}

func TestWalkOperation_ExclusionOperation(t *testing.T) {
	op := &ExclusionOperation{
		left:  &RelationReference{relationName: "viewer"},
		right: &RelationReference{relationName: "banned"},
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 3) // 1 exclusion + 2 children
	require.Len(t, visitor.exclusionOps, 1)
	require.Len(t, visitor.relationReferences, 2)
}

func TestWalkOperation_ComplexNestedOperation(t *testing.T) {
	// Build a complex operation tree:
	// (viewer | editor) & approved - banned
	op := &ExclusionOperation{
		left: &IntersectionOperation{
			children: []Operation{
				&UnionOperation{
					children: []Operation{
						&RelationReference{relationName: "viewer"},
						&RelationReference{relationName: "editor"},
					},
				},
				&RelationReference{relationName: "approved"},
			},
		},
		right: &RelationReference{relationName: "banned"},
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.exclusionOps, 1)
	require.Len(t, visitor.intersectionOps, 1)
	require.Len(t, visitor.unionOperations, 1)
	require.Len(t, visitor.relationReferences, 4)
	require.Len(t, visitor.operations, 7) // 1 exclusion + 1 intersection + 1 union + 4 relation refs
}

func TestWalkOperation_WithArrowInComplex(t *testing.T) {
	// Build: parent->viewer | editor
	op := &UnionOperation{
		children: []Operation{
			&ArrowReference{
				left:  "parent",
				right: "viewer",
			},
			&RelationReference{relationName: "editor"},
		},
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.unionOperations, 1)
	require.Len(t, visitor.arrowReferences, 1)
	require.Len(t, visitor.relationReferences, 1)
	require.Len(t, visitor.operations, 3)
}

func TestWalkSchema_CompleteSchemaTraversal(t *testing.T) {
	// Build a complete schema with all node types
	br1 := &BaseRelation{
		subjectType: "user",
	}
	rel1 := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{br1},
	}
	br1.parent = rel1

	perm1 := &Permission{
		name: "view",
		operation: &UnionOperation{
			children: []Operation{
				&RelationReference{relationName: "viewer"},
				&ArrowReference{left: "parent", right: "view"},
			},
		},
	}

	def1 := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel1,
		},
		permissions: map[string]*Permission{
			"view": perm1,
		},
	}
	rel1.parent = def1
	perm1.parent = def1

	caveat1 := &Caveat{
		name:       "is_admin",
		expression: "admin == true",
	}

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def1,
		},
		caveats: map[string]*Caveat{
			"is_admin": caveat1,
		},
	}
	def1.parent = schema
	caveat1.parent = schema

	visitor := &testVisitor{}
	_, err := WalkSchema(schema, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.schemas, 1)
	require.Len(t, visitor.definitions, 1)
	require.Len(t, visitor.caveats, 1)
	require.Len(t, visitor.relations, 1)
	require.Len(t, visitor.baseRelations, 1)
	require.Len(t, visitor.permissions, 1)
	require.Len(t, visitor.unionOperations, 1)
	require.Len(t, visitor.relationReferences, 1)
	require.Len(t, visitor.arrowReferences, 1)
}

// partialVisitor only implements SchemaVisitor and DefinitionVisitor
type partialVisitor struct {
	schemas     []*Schema
	definitions []*Definition
}

func (pv *partialVisitor) VisitSchema(s *Schema, value struct{}) (struct{}, bool, error) {
	pv.schemas = append(pv.schemas, s)
	return value, true, nil
}

func (pv *partialVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	pv.definitions = append(pv.definitions, d)
	return value, true, nil
}

func TestWalkSchema_PartialVisitor(t *testing.T) {
	// Test with a visitor that only implements some interfaces
	pv := &partialVisitor{}

	schema := &Schema{
		definitions: map[string]*Definition{
			"user": {
				name:        "user",
				relations:   map[string]*Relation{},
				permissions: map[string]*Permission{},
			},
		},
		caveats: make(map[string]*Caveat),
	}
	schema.definitions["user"].parent = schema

	_, err := WalkSchema(schema, pv, struct{}{})
	require.NoError(t, err)

	// The walk should complete without panic even though visitor doesn't implement all interfaces
	// Should have visited the schema and definition
	require.Len(t, pv.schemas, 1)
	require.Len(t, pv.definitions, 1)
}

type errorVisitor struct{}

func (ev *errorVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	if d.Name() == "document" {
		return value, false, errTestError
	}
	return value, true, nil
}

func TestWalkSchema_ErrorPropagation(t *testing.T) {
	schema := &Schema{
		definitions: map[string]*Definition{
			"user": {
				name:        "user",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
			"document": {
				name:        "document",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
		},
		caveats: make(map[string]*Caveat),
	}
	schema.definitions["user"].parent = schema
	schema.definitions["document"].parent = schema

	ev := &errorVisitor{}
	_, err := WalkSchema(schema, ev, struct{}{})

	require.Error(t, err)
	require.Equal(t, errTestError, err)
}

type stopVisitor struct {
	visitedSchema bool
	visitedDefs   int
}

func (sv *stopVisitor) VisitSchema(s *Schema, value struct{}) (struct{}, bool, error) {
	sv.visitedSchema = true
	return value, false, nil // Stop walking
}

func (sv *stopVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	sv.visitedDefs++
	return value, true, nil
}

func TestWalkSchema_EarlyTerminationOnSchema(t *testing.T) {
	schema := &Schema{
		definitions: map[string]*Definition{
			"user": {
				name:        "user",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
		},
		caveats: make(map[string]*Caveat),
	}
	schema.definitions["user"].parent = schema

	sv := &stopVisitor{}
	_, err := WalkSchema(schema, sv, struct{}{})

	require.NoError(t, err)
	require.True(t, sv.visitedSchema)
	require.Equal(t, 0, sv.visitedDefs) // Should not have visited definitions
}

type stopDefVisitor struct {
	visitedDef       bool
	visitedRelations int
}

func (sv *stopDefVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	sv.visitedDef = true
	return value, false, nil // Stop walking
}

func (sv *stopDefVisitor) VisitRelation(r *Relation, value struct{}) (struct{}, bool, error) {
	sv.visitedRelations++
	return value, true, nil
}

func TestWalkDefinition_EarlyTermination(t *testing.T) {
	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}
	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: make(map[string]*Permission),
	}
	rel.parent = def

	sv := &stopDefVisitor{}
	_, err := WalkDefinition(def, sv, struct{}{})

	require.NoError(t, err)
	require.True(t, sv.visitedDef)
	require.Equal(t, 0, sv.visitedRelations) // Should not have visited relations
}

type errorOnSecondVisitor struct {
	count int
}

func (ev *errorOnSecondVisitor) VisitOperation(op Operation, value struct{}) (struct{}, bool, error) {
	ev.count++
	if ev.count == 2 {
		return value, false, errTestError
	}
	return value, true, nil
}

func TestWalkOperation_ErrorInNestedOperation(t *testing.T) {
	op := &UnionOperation{
		children: []Operation{
			&RelationReference{relationName: "viewer"},
			&RelationReference{relationName: "editor"},
			&RelationReference{relationName: "admin"},
		},
	}

	ev := &errorOnSecondVisitor{}
	_, err := WalkOperation(op, ev, struct{}{})

	require.Error(t, err)
	require.Equal(t, errTestError, err)
	require.Equal(t, 2, ev.count)
}

type stopOnUnionVisitor struct {
	visitedUnion    bool
	visitedChildren int
}

func (sv *stopOnUnionVisitor) VisitUnionOperation(uo *UnionOperation, value struct{}) (struct{}, bool, error) {
	sv.visitedUnion = true
	return value, false, nil // Stop walking children
}

func (sv *stopOnUnionVisitor) VisitRelationReference(rr *RelationReference, value struct{}) (struct{}, error) {
	sv.visitedChildren++
	return value, nil
}

func TestWalkOperation_EarlyTerminationInUnion(t *testing.T) {
	op := &UnionOperation{
		children: []Operation{
			&RelationReference{relationName: "viewer"},
			&RelationReference{relationName: "editor"},
		},
	}

	sv := &stopOnUnionVisitor{}
	_, err := WalkOperation(op, sv, struct{}{})

	require.NoError(t, err)
	require.True(t, sv.visitedUnion)
	require.Equal(t, 0, sv.visitedChildren) // Should not have visited children
}

type countingVisitor struct {
	operationCount int
	relRefCount    int
}

func (cv *countingVisitor) VisitOperation(op Operation, value struct{}) (struct{}, bool, error) {
	cv.operationCount++
	return value, true, nil // Continue to specific operation handlers
}

func (cv *countingVisitor) VisitRelationReference(rr *RelationReference, value struct{}) (struct{}, error) {
	cv.relRefCount++
	return value, nil
}

func TestWalkOperation_ContinueThroughOperationVisitor(t *testing.T) {
	op := &UnionOperation{
		children: []Operation{
			&RelationReference{relationName: "viewer"},
			&RelationReference{relationName: "editor"},
		},
	}

	cv := &countingVisitor{}
	_, err := WalkOperation(op, cv, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 3, cv.operationCount) // 1 union + 2 relation refs
	require.Equal(t, 2, cv.relRefCount)
}

// intVisitor implements all visitor interfaces and increments an int value
type intVisitor struct{}

func (iv *intVisitor) VisitSchema(s *Schema, value int) (int, bool, error) {
	return value + 1, true, nil
}

func (iv *intVisitor) VisitDefinition(d *Definition, value int) (int, bool, error) {
	return value + 10, true, nil
}

func (iv *intVisitor) VisitCaveat(c *Caveat, value int) (int, error) {
	return value + 100, nil
}

func (iv *intVisitor) VisitRelation(r *Relation, value int) (int, bool, error) {
	return value + 1000, true, nil
}

func (iv *intVisitor) VisitBaseRelation(br *BaseRelation, value int) (int, error) {
	return value + 10000, nil
}

func (iv *intVisitor) VisitPermission(p *Permission, value int) (int, bool, error) {
	return value + 100000, true, nil
}

func (iv *intVisitor) VisitOperation(op Operation, value int) (int, bool, error) {
	return value + 1000000, true, nil
}

func (iv *intVisitor) VisitRelationReference(rr *RelationReference, value int) (int, error) {
	return value + 10000000, nil
}

func (iv *intVisitor) VisitArrowReference(ar *ArrowReference, value int) (int, error) {
	return value + 100000000, nil
}

func (iv *intVisitor) VisitUnionOperation(uo *UnionOperation, value int) (int, bool, error) {
	return value + 2, true, nil
}

func (iv *intVisitor) VisitIntersectionOperation(io *IntersectionOperation, value int) (int, bool, error) {
	return value + 3, true, nil
}

func (iv *intVisitor) VisitExclusionOperation(eo *ExclusionOperation, value int) (int, bool, error) {
	return value + 4, true, nil
}

func TestWalkSchema_ValueThreading(t *testing.T) {
	schema := &Schema{
		definitions: map[string]*Definition{
			"user": {
				name:        "user",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
		},
		caveats: make(map[string]*Caveat),
	}
	schema.definitions["user"].parent = schema

	visitor := &intVisitor{}
	result, err := WalkSchema(schema, visitor, 0)
	require.NoError(t, err)

	// Should have visited: Schema (+1), Definition (+10) = 11
	require.Equal(t, 11, result)
}

func TestWalkDefinition_ValueThreading(t *testing.T) {
	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{{subjectType: "user"}},
	}
	perm := &Permission{
		name:      "view",
		operation: &RelationReference{relationName: "viewer"},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.parent = def
	rel.baseRelations[0].parent = rel
	perm.parent = def

	visitor := &intVisitor{}
	result, err := WalkDefinition(def, visitor, 0)
	require.NoError(t, err)

	// Should have visited: Definition (+10), Relation (+1000), BaseRelation (+10000),
	// Permission (+100000), Operation (+1000000), RelationReference (+10000000) = 11,111,010
	require.Equal(t, 11111010, result)
}

func TestWalkOperation_ValueThreading(t *testing.T) {
	// Build: (viewer | editor) & approved
	op := &IntersectionOperation{
		children: []Operation{
			&UnionOperation{
				children: []Operation{
					&RelationReference{relationName: "viewer"},
					&RelationReference{relationName: "editor"},
				},
			},
			&RelationReference{relationName: "approved"},
		},
	}

	visitor := &intVisitor{}
	result, err := WalkOperation(op, visitor, 0)
	require.NoError(t, err)

	// Should have visited:
	// - IntersectionOperation: +1000000 (OperationVisitor) +3 (IntersectionOperationVisitor) = 1,000,003
	// - UnionOperation: +1000000 (OperationVisitor) +2 (UnionOperationVisitor) = 1,000,002
	// - RelationReference (viewer): +1000000 (OperationVisitor) +10000000 (RelationReferenceVisitor) = 11,000,000
	// - RelationReference (editor): +1000000 (OperationVisitor) +10000000 (RelationReferenceVisitor) = 11,000,000
	// - RelationReference (approved): +1000000 (OperationVisitor) +10000000 (RelationReferenceVisitor) = 11,000,000
	// Total: 1,000,003 + 1,000,002 + 11,000,000 + 11,000,000 + 11,000,000 = 35,000,005
	require.Equal(t, 35000005, result)
}

func TestWalkOperation_ExclusionValueThreading(t *testing.T) {
	// Build: viewer - banned
	op := &ExclusionOperation{
		left:  &RelationReference{relationName: "viewer"},
		right: &RelationReference{relationName: "banned"},
	}

	visitor := &intVisitor{}
	result, err := WalkOperation(op, visitor, 0)
	require.NoError(t, err)

	// Should have visited:
	// - ExclusionOperation: +1000000 (OperationVisitor) +4 (ExclusionOperationVisitor) = 1,000,004
	// - RelationReference (viewer): +1000000 (OperationVisitor) +10000000 (RelationReferenceVisitor) = 11,000,000
	// - RelationReference (banned): +1000000 (OperationVisitor) +10000000 (RelationReferenceVisitor) = 11,000,000
	// Total: 1,000,004 + 11,000,000 + 11,000,000 = 23,000,004
	require.Equal(t, 23000004, result)
}

func TestArrowOperationInterface(t *testing.T) {
	tests := []struct {
		name             string
		operation        ArrowOperation
		expectedLeft     string
		expectedRight    string
		expectedFunction FunctionType
	}{
		{
			name: "ArrowReference",
			operation: &ArrowReference{
				left:  "parent",
				right: "viewer",
			},
			expectedLeft:     "parent",
			expectedRight:    "viewer",
			expectedFunction: FunctionTypeAny,
		},
		{
			name: "FunctionedArrowReference with any",
			operation: &FunctionedArrowReference{
				left:     "parent",
				right:    "viewer",
				function: FunctionTypeAny,
			},
			expectedLeft:     "parent",
			expectedRight:    "viewer",
			expectedFunction: FunctionTypeAny,
		},
		{
			name: "FunctionedArrowReference with all",
			operation: &FunctionedArrowReference{
				left:     "parent",
				right:    "viewer",
				function: FunctionTypeAll,
			},
			expectedLeft:     "parent",
			expectedRight:    "viewer",
			expectedFunction: FunctionTypeAll,
		},
		{
			name: "ResolvedArrowReference",
			operation: &ResolvedArrowReference{
				left:         "parent",
				resolvedLeft: &Relation{name: "parent"},
				right:        "viewer",
			},
			expectedLeft:     "parent",
			expectedRight:    "viewer",
			expectedFunction: FunctionTypeAny,
		},
		{
			name: "ResolvedFunctionedArrowReference with any",
			operation: &ResolvedFunctionedArrowReference{
				left:         "parent",
				resolvedLeft: &Relation{name: "parent"},
				right:        "viewer",
				function:     FunctionTypeAny,
			},
			expectedLeft:     "parent",
			expectedRight:    "viewer",
			expectedFunction: FunctionTypeAny,
		},
		{
			name: "ResolvedFunctionedArrowReference with all",
			operation: &ResolvedFunctionedArrowReference{
				left:         "parent",
				resolvedLeft: &Relation{name: "parent"},
				right:        "viewer",
				function:     FunctionTypeAll,
			},
			expectedLeft:     "parent",
			expectedRight:    "viewer",
			expectedFunction: FunctionTypeAll,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectedLeft, tt.operation.Left())
			require.Equal(t, tt.expectedRight, tt.operation.Right())
			require.Equal(t, tt.expectedFunction, tt.operation.Function())

			// Verify it implements Operation interface
			var _ Operation = tt.operation
		})
	}
}

// arrowCounter counts arrow operations by function type
type arrowCounter struct {
	anyCount int
	allCount int
	arrows   []ArrowOperation
}

func (ac *arrowCounter) VisitArrowOperation(ao ArrowOperation, value struct{}) (struct{}, error) {
	ac.arrows = append(ac.arrows, ao)
	if ao.Function() == FunctionTypeAny {
		ac.anyCount++
	} else if ao.Function() == FunctionTypeAll {
		ac.allCount++
	}
	return value, nil
}

func TestArrowOperationVisitor(t *testing.T) {
	// Create a test schema with different arrow types
	rel := &Relation{
		name:          "parent",
		baseRelations: []*BaseRelation{},
	}

	perm := &Permission{
		name: "view",
		operation: &UnionOperation{
			children: []Operation{
				&ArrowReference{left: "parent", right: "viewer"},
				&FunctionedArrowReference{left: "parent", right: "editor", function: FunctionTypeAny},
				&FunctionedArrowReference{left: "parent", right: "admin", function: FunctionTypeAll},
			},
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"parent": rel,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Create a visitor that counts arrow operations by function type
	visitor := &arrowCounter{}

	// Walk the schema and count arrows
	_, err = WalkSchema(resolved.Schema(), visitor, struct{}{})
	require.NoError(t, err)

	// Verify we found all three arrows
	require.Len(t, visitor.arrows, 3)
	require.Equal(t, 2, visitor.anyCount) // 2 "any" arrows (including standard arrow)
	require.Equal(t, 1, visitor.allCount) // 1 "all" arrow
}

// arrowCollector collects all arrow operations
type arrowCollector struct {
	arrows []struct {
		left     string
		right    string
		function FunctionType
	}
}

func (ac *arrowCollector) VisitArrowOperation(ao ArrowOperation, value struct{}) (struct{}, error) {
	ac.arrows = append(ac.arrows, struct {
		left     string
		right    string
		function FunctionType
	}{
		left:     ao.Left(),
		right:    ao.Right(),
		function: ao.Function(),
	})
	return value, nil
}

func TestArrowOperationVisitor_WithCompilerSchema(t *testing.T) {
	schemaString := `definition document {
	relation parent: folder
	relation owner: folder
	permission view = parent->viewer + parent.any(editor) + owner.all(admin)
}

definition folder {
	relation viewer: user
	relation editor: user
	relation admin: user
}

definition user {}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Create a visitor that collects all arrow operations
	visitor := &arrowCollector{}

	// Walk the schema
	_, err = WalkSchema(resolved.Schema(), visitor, struct{}{})
	require.NoError(t, err)

	// Verify we found all three arrows
	require.Len(t, visitor.arrows, 3)

	// Verify the arrows have the correct properties
	require.Equal(t, "parent", visitor.arrows[0].left)
	require.Equal(t, "viewer", visitor.arrows[0].right)
	require.Equal(t, FunctionTypeAny, visitor.arrows[0].function)

	require.Equal(t, "parent", visitor.arrows[1].left)
	require.Equal(t, "editor", visitor.arrows[1].right)
	require.Equal(t, FunctionTypeAny, visitor.arrows[1].function)

	require.Equal(t, "owner", visitor.arrows[2].left)
	require.Equal(t, "admin", visitor.arrows[2].right)
	require.Equal(t, FunctionTypeAll, visitor.arrows[2].function)
}

func TestWalkOperation_UnionWithNilReference(t *testing.T) {
	op := &UnionOperation{
		children: []Operation{
			&RelationReference{relationName: "viewer"},
			&NilReference{},
			&RelationReference{relationName: "editor"},
		},
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 4) // 1 union + 3 children
	require.Len(t, visitor.unionOperations, 1)
	require.Len(t, visitor.relationReferences, 2)
	require.Len(t, visitor.nilReferences, 1)
}

type nilRefErrorVisitor struct{}

func (ev *nilRefErrorVisitor) VisitOperation(op Operation, value struct{}) (struct{}, bool, error) {
	return value, true, nil
}

func (ev *nilRefErrorVisitor) VisitNilReference(nr *NilReference, value struct{}) (struct{}, error) {
	return value, errTestError
}

func TestNilReferenceVisitor_ErrorHandling(t *testing.T) {
	op := &NilReference{}
	visitor := &nilRefErrorVisitor{}

	_, err := WalkOperation(op, visitor, struct{}{})
	require.Error(t, err)
	require.Equal(t, errTestError, err)
}
