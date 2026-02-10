package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
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

// orderTrackingVisitor tracks the order of visited nodes by their names
type orderTrackingVisitor struct {
	visitOrder []string
}

func (otv *orderTrackingVisitor) VisitSchema(s *Schema, value struct{}) (struct{}, bool, error) {
	otv.visitOrder = append(otv.visitOrder, "schema")
	return value, true, nil
}

func (otv *orderTrackingVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	otv.visitOrder = append(otv.visitOrder, "def:"+d.name)
	return value, true, nil
}

func (otv *orderTrackingVisitor) VisitRelation(r *Relation, value struct{}) (struct{}, bool, error) {
	otv.visitOrder = append(otv.visitOrder, "rel:"+r.name)
	return value, true, nil
}

func (otv *orderTrackingVisitor) VisitBaseRelation(br *BaseRelation, value struct{}) (struct{}, error) {
	otv.visitOrder = append(otv.visitOrder, "baserel:"+br.subjectType)
	return value, nil
}

func (otv *orderTrackingVisitor) VisitPermission(p *Permission, value struct{}) (struct{}, bool, error) {
	otv.visitOrder = append(otv.visitOrder, "perm:"+p.name)
	return value, true, nil
}

func (otv *orderTrackingVisitor) VisitRelationReference(rr *RelationReference, value struct{}) (struct{}, error) {
	otv.visitOrder = append(otv.visitOrder, "relref:"+rr.relationName)
	return value, nil
}

func (otv *orderTrackingVisitor) VisitUnionOperation(uo *UnionOperation, value struct{}) (struct{}, bool, error) {
	otv.visitOrder = append(otv.visitOrder, "union")
	return value, true, nil
}

func (otv *orderTrackingVisitor) VisitIntersectionOperation(io *IntersectionOperation, value struct{}) (struct{}, bool, error) {
	otv.visitOrder = append(otv.visitOrder, "intersection")
	return value, true, nil
}

func (otv *orderTrackingVisitor) VisitExclusionOperation(eo *ExclusionOperation, value struct{}) (struct{}, bool, error) {
	otv.visitOrder = append(otv.visitOrder, "exclusion")
	return value, true, nil
}

// TestWalkSchema_PreOrderVsPostOrder verifies that PreOrder and PostOrder produce different traversal orders
func TestWalkSchema_PreOrderVsPostOrder(t *testing.T) {
	// Build a simple schema with nested structure
	schema := buildTestSchema(t)

	// Test PreOrder traversal (default)
	preOrderVisitor := &orderTrackingVisitor{}
	_, err := WalkSchema(schema, preOrderVisitor, struct{}{})
	require.NoError(t, err)

	// Test PostOrder traversal
	postOrderVisitor := &orderTrackingVisitor{}
	_, err = WalkSchemaWithOptions(schema, postOrderVisitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.NoError(t, err)

	// Verify orders are different
	require.NotEqual(t, preOrderVisitor.visitOrder, postOrderVisitor.visitOrder)

	// Verify PreOrder visits parent before children
	// Schema should be first in PreOrder
	require.Equal(t, "schema", preOrderVisitor.visitOrder[0])

	// Verify PostOrder visits children before parent
	// Schema should be last in PostOrder
	require.Equal(t, "schema", postOrderVisitor.visitOrder[len(postOrderVisitor.visitOrder)-1])
}

// TestWalkDefinition_PreOrderVsPostOrder tests order difference at definition level
func TestWalkDefinition_PreOrderVsPostOrder(t *testing.T) {
	schema := buildTestSchema(t)
	def := schema.definitions["document"]
	require.NotNil(t, def)

	// PreOrder
	preOrderVisitor := &orderTrackingVisitor{}
	_, err := WalkDefinition(def, preOrderVisitor, struct{}{})
	require.NoError(t, err)

	// PostOrder
	postOrderVisitor := &orderTrackingVisitor{}
	_, err = WalkDefinitionWithOptions(def, postOrderVisitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.NoError(t, err)

	// In PreOrder, definition should be first
	require.Equal(t, "def:document", preOrderVisitor.visitOrder[0])

	// In PostOrder, definition should be last
	require.Equal(t, "def:document", postOrderVisitor.visitOrder[len(postOrderVisitor.visitOrder)-1])
}

// TestWalkOperation_PreOrderVsPostOrder tests operation tree traversal order
func TestWalkOperation_PreOrderVsPostOrder(t *testing.T) {
	// Build operation tree: (a | b) & c
	op := &IntersectionOperation{
		children: []Operation{
			&UnionOperation{
				children: []Operation{
					&RelationReference{relationName: "a"},
					&RelationReference{relationName: "b"},
				},
			},
			&RelationReference{relationName: "c"},
		},
	}

	// PreOrder: should visit intersection, then union, then leaves
	preOrderVisitor := &orderTrackingVisitor{}
	_, err := WalkOperation(op, preOrderVisitor, struct{}{})
	require.NoError(t, err)

	// Expected PreOrder: intersection, union, a, b, c
	expectedPreOrder := []string{"intersection", "union", "relref:a", "relref:b", "relref:c"}
	require.Equal(t, expectedPreOrder, preOrderVisitor.visitOrder)

	// PostOrder: should visit leaves first, then union, then intersection
	postOrderVisitor := &orderTrackingVisitor{}
	_, err = WalkOperationWithOptions(op, postOrderVisitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.NoError(t, err)

	// Expected PostOrder: a, b, union, c, intersection (like postfix notation)
	expectedPostOrder := []string{"relref:a", "relref:b", "union", "relref:c", "intersection"}
	require.Equal(t, expectedPostOrder, postOrderVisitor.visitOrder)
}

// valueThreadingVisitor tracks value threading by accumulating counts
type valueThreadingVisitor struct {
	schemaCount     int
	definitionCount int
	relationCount   int
	permissionCount int
}

func (vtv *valueThreadingVisitor) VisitSchema(s *Schema, value int) (int, bool, error) {
	vtv.schemaCount++
	return value + 1, true, nil
}

func (vtv *valueThreadingVisitor) VisitDefinition(d *Definition, value int) (int, bool, error) {
	vtv.definitionCount++
	return value + 10, true, nil
}

func (vtv *valueThreadingVisitor) VisitRelation(r *Relation, value int) (int, bool, error) {
	vtv.relationCount++
	return value + 100, true, nil
}

func (vtv *valueThreadingVisitor) VisitPermission(p *Permission, value int) (int, bool, error) {
	vtv.permissionCount++
	return value + 1000, true, nil
}

// TestWalkSchema_PostOrderValueThreading verifies value threading works correctly in PostOrder
func TestWalkSchema_PostOrderValueThreading(t *testing.T) {
	schema := buildTestSchema(t)

	// Test with PreOrder (baseline)
	preOrderVisitor := &valueThreadingVisitor{}
	preOrderResult, err := WalkSchema(schema, preOrderVisitor, 0)
	require.NoError(t, err)

	// Test with PostOrder
	postOrderVisitor := &valueThreadingVisitor{}
	postOrderResult, err := WalkSchemaWithOptions(schema, postOrderVisitor, 0, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.NoError(t, err)

	// Both should have visited the same nodes
	require.Equal(t, preOrderVisitor.schemaCount, postOrderVisitor.schemaCount)
	require.Equal(t, preOrderVisitor.definitionCount, postOrderVisitor.definitionCount)
	require.Equal(t, preOrderVisitor.relationCount, postOrderVisitor.relationCount)
	require.Equal(t, preOrderVisitor.permissionCount, postOrderVisitor.permissionCount)

	// Both should have the same final accumulated value
	require.Equal(t, preOrderResult, postOrderResult)
}

// continueTestVisitor stops at definitions to test continue flag behavior
type continueTestVisitor struct {
	definitionsSeen int
	relationsSeen   int
	stopAtFirstDef  bool
}

func (ctv *continueTestVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	ctv.definitionsSeen++
	if ctv.stopAtFirstDef && ctv.definitionsSeen == 1 {
		return value, false, nil // Stop - don't visit children
	}
	return value, true, nil
}

func (ctv *continueTestVisitor) VisitRelation(r *Relation, value struct{}) (struct{}, bool, error) {
	ctv.relationsSeen++
	return value, true, nil
}

// TestWalkDefinition_ContinueFlagInPreOrder verifies continue=false skips children in PreOrder
func TestWalkDefinition_ContinueFlagInPreOrder(t *testing.T) {
	schema := buildTestSchema(t)
	def := schema.definitions["document"]

	visitor := &continueTestVisitor{stopAtFirstDef: true}
	_, err := WalkDefinition(def, visitor, struct{}{})
	require.NoError(t, err)

	// In PreOrder, continue=false should skip children
	require.Equal(t, 1, visitor.definitionsSeen)
	require.Equal(t, 0, visitor.relationsSeen) // Children not visited
}

// TestWalkDefinition_ContinueFlagInPostOrder verifies children are visited regardless in PostOrder
func TestWalkDefinition_ContinueFlagInPostOrder(t *testing.T) {
	schema := buildTestSchema(t)
	def := schema.definitions["document"]

	visitor := &continueTestVisitor{stopAtFirstDef: true}
	_, err := WalkDefinitionWithOptions(def, visitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.NoError(t, err)

	// In PostOrder, children are visited before parent, so continue flag doesn't affect them
	require.Equal(t, 1, visitor.definitionsSeen)
	require.Positive(t, visitor.relationsSeen) // Children were visited
}

// errorInChildVisitor returns error from a relation to test error propagation
type errorInChildVisitor struct{}

func (ev *errorInChildVisitor) VisitRelation(r *Relation, value struct{}) (struct{}, bool, error) {
	if r.name == "viewer" {
		return value, false, errTestError
	}
	return value, true, nil
}

func (ev *errorInChildVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	return value, true, nil
}

// TestWalkDefinition_ErrorPropagationInPostOrder verifies errors propagate correctly in PostOrder
func TestWalkDefinition_ErrorPropagationInPostOrder(t *testing.T) {
	schema := buildTestSchema(t)
	def := schema.definitions["document"]

	visitor := &errorInChildVisitor{}
	_, err := WalkDefinitionWithOptions(def, visitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.Error(t, err)
	require.Equal(t, errTestError, err)
}

// TestWalkSchema_BackwardCompatibility verifies default behavior is PreOrder
func TestWalkSchema_BackwardCompatibility(t *testing.T) {
	schema := buildTestSchema(t)

	// Default Walk (no options) should behave like PreOrder
	defaultVisitor := &orderTrackingVisitor{}
	_, err := WalkSchema(schema, defaultVisitor, struct{}{})
	require.NoError(t, err)

	// Explicit PreOrder
	preOrderVisitor := &orderTrackingVisitor{}
	_, err = WalkSchemaWithOptions(schema, preOrderVisitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPreOrder))
	require.NoError(t, err)

	// Both should visit the same number of nodes
	require.Len(t, defaultVisitor.visitOrder, len(preOrderVisitor.visitOrder))

	// Both should have schema as the first node (PreOrder characteristic)
	require.Equal(t, "schema", defaultVisitor.visitOrder[0])
	require.Equal(t, "schema", preOrderVisitor.visitOrder[0])

	// PostOrder should be different - schema should be last
	postOrderVisitor := &orderTrackingVisitor{}
	_, err = WalkSchemaWithOptions(schema, postOrderVisitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.NoError(t, err)

	require.Equal(t, "schema", postOrderVisitor.visitOrder[len(postOrderVisitor.visitOrder)-1])
	require.NotEqual(t, defaultVisitor.visitOrder[0], postOrderVisitor.visitOrder[0])
}

// buildTestSchema creates a test schema with nested structure for testing
func buildTestSchema(t *testing.T) *Schema {
	// Create base relations
	viewerBR := &BaseRelation{subjectType: "user"}
	editorBR := &BaseRelation{subjectType: "user"}

	// Create relations
	viewerRel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{viewerBR},
	}
	editorRel := &Relation{
		name:          "editor",
		baseRelations: []*BaseRelation{editorBR},
	}

	// Create permissions with union operation
	viewPerm := &Permission{
		name: "view",
		operation: &UnionOperation{
			children: []Operation{
				&RelationReference{relationName: "viewer"},
				&RelationReference{relationName: "editor"},
			},
		},
	}
	editPerm := &Permission{
		name:      "edit",
		operation: &RelationReference{relationName: "editor"},
	}

	// Create definition
	docDef := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": viewerRel,
			"editor": editorRel,
		},
		permissions: map[string]*Permission{
			"view": viewPerm,
			"edit": editPerm,
		},
	}

	// Set parent references
	viewerBR.parent = viewerRel
	editorBR.parent = editorRel
	viewerRel.parent = docDef
	editorRel.parent = docDef
	viewPerm.parent = docDef
	editPerm.parent = docDef

	// Create schema
	schema := &Schema{
		definitions: map[string]*Definition{
			"document": docDef,
		},
		caveats: make(map[string]*Caveat),
	}
	docDef.parent = schema

	return schema
}

// TestOperationParentPointers verifies that parent pointers are correctly set
// for operations in the tree when built using the schema builder.
func TestOperationParentPointers(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("view").
		UnionExpr().
		AddRelationRef("owner").
		AddRelationRef("viewer").
		Done().
		Done().
		Done().
		Build()

	def, ok := schema.GetTypeDefinition("document")
	require.True(t, ok)
	perm, ok := def.GetPermission("view")
	require.True(t, ok)
	op := perm.Operation()

	// Check that child operations have the union as their parent
	union, ok := op.(*UnionOperation)
	require.True(t, ok, "Expected UnionOperation")

	require.Equal(t, perm, union.Parent(), "Union's parent should be the permission")

	children := union.Children()
	require.Len(t, children, 2)

	for i, child := range children {
		require.Equal(t, union, child.Parent(), "Child %d should have union as parent", i)
	}
}

// TestOperationParentPointersNestedOperations verifies parent pointers in deeply nested operations.
func TestOperationParentPointersNestedOperations(t *testing.T) {
	// Build a schema with deeply nested operations: (a | b) & c
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("complex").
		Intersection(
			Union(
				RelRef("a"),
				RelRef("b"),
			),
			RelRef("c"),
		).
		Done().
		Done().
		Build()

	def, ok := schema.GetTypeDefinition("document")
	require.True(t, ok)
	perm, ok := def.GetPermission("complex")
	require.True(t, ok)
	op := perm.Operation()

	// The root operation (intersection) should have the permission as parent
	require.Equal(t, perm, op.Parent(), "Root operation's parent should be the permission")

	intersection, ok := op.(*IntersectionOperation)
	require.True(t, ok, "Expected IntersectionOperation")

	children := intersection.Children()
	require.Len(t, children, 2)

	// First child is a union
	union, ok := children[0].(*UnionOperation)
	require.True(t, ok, "Expected first child to be UnionOperation")

	// Union's parent should be the intersection
	require.Equal(t, intersection, union.Parent(), "Union's parent should be intersection")

	// Union's children should have union as parent
	unionChildren := union.Children()
	for i, child := range unionChildren {
		require.Equal(t, union, child.Parent(), "Union child %d should have union as parent", i)
	}

	// Second child of intersection (RelRef("c")) should have intersection as parent
	require.Equal(t, intersection, children[1].Parent(), "Second intersection child should have intersection as parent")
}

// TestOperationParentPointersExclusion verifies parent pointers in exclusion operations.
func TestOperationParentPointersExclusion(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("restricted").
		Exclusion(
			Union(RelRef("a"), RelRef("b")),
			RelRef("c"),
		).
		Done().
		Done().
		Build()

	def, ok := schema.GetTypeDefinition("document")
	require.True(t, ok)
	perm, ok := def.GetPermission("restricted")
	require.True(t, ok)
	op := perm.Operation()

	// Root operation (exclusion) should have the permission as parent
	require.Equal(t, perm, op.Parent())

	exclusion, ok := op.(*ExclusionOperation)
	require.True(t, ok)

	// Left side (union) should have exclusion as parent
	leftOp := exclusion.Left()
	require.Equal(t, exclusion, leftOp.Parent())

	// Right side should have exclusion as parent
	rightOp := exclusion.Right()
	require.Equal(t, exclusion, rightOp.Parent())

	// Children of the union should have union as parent
	union, ok := leftOp.(*UnionOperation)
	require.True(t, ok)
	for _, child := range union.Children() {
		require.Equal(t, union, child.Parent())
	}
}

// TestOperationParentPointersPostOrderTraversal verifies that parent pointers
// are accessible and correctly set during post-order traversal.
func TestOperationParentPointersPostOrderTraversal(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("view").
		UnionExpr().
		AddRelationRef("owner").
		AddRelationRef("viewer").
		Done().
		Done().
		Done().
		Build()

	def, ok := schema.GetTypeDefinition("document")
	require.True(t, ok)
	perm, ok := def.GetPermission("view")
	require.True(t, ok)

	// Manually verify parent pointers during post-order traversal
	union, ok := perm.Operation().(*UnionOperation)
	require.True(t, ok)

	// The union should have the permission as parent (it's the root)
	require.Equal(t, perm, union.Parent())

	// Each child should have the union as parent
	children := union.Children()
	for _, child := range children {
		require.Equal(t, union, child.Parent())
	}

	// Walk in post-order and verify we can access parents
	visitor := &testVisitor{}
	_, err := WalkOperationWithOptions(perm.Operation(), visitor, struct{}{}, NewWalkOptions().WithStrategy(WalkPostOrder))
	require.NoError(t, err)

	// Verify that operations were visited
	require.NotEmpty(t, visitor.operations)
	require.NotEmpty(t, visitor.relationReferences)

	// Verify each visited operation has the correct parent
	for _, relRef := range visitor.relationReferences {
		require.NotNil(t, relRef.Parent(), "RelationReference should have a parent")
		require.IsType(t, &UnionOperation{}, relRef.Parent(), "RelationReference parent should be UnionOperation")
	}
}

func TestOperationParentWithPermission(t *testing.T) {
	// Build a schema with a permission that has nested operations
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("view").Union(
		&RelationReference{relationName: "viewer"},
		&ArrowReference{left: "parent", right: "view"},
	).Done().
		AddPermission("edit").Intersection(
		&RelationReference{relationName: "editor"},
		&RelationReference{relationName: "owner"},
	).Done().
		Done().
		Build()

	def, found := schema.GetTypeDefinition("document")
	require.True(t, found, "definition should exist")
	require.NotNil(t, def, "definition should not be nil")

	viewPerm, found := def.GetPermission("view")
	require.True(t, found, "view permission should exist")
	require.NotNil(t, viewPerm, "view permission should not be nil")

	editPerm, found := def.GetPermission("edit")
	require.True(t, found, "edit permission should exist")
	require.NotNil(t, editPerm, "edit permission should not be nil")

	// Verify that the ROOT operation has the permission as parent
	viewOp := viewPerm.Operation()
	require.NotNil(t, viewOp, "view operation should exist")
	require.Equal(t, viewPerm, viewOp.Parent(), "root operation's parent should be the permission")

	// Verify that child operations have correct parent
	if unionOp, ok := viewOp.(*UnionOperation); ok {
		for _, child := range unionOp.Children() {
			require.Equal(t, unionOp, child.Parent(), "child operation should have union as parent")
		}
	}

	// Verify that the ROOT operation in edit has the permission as parent
	editOp := editPerm.Operation()
	require.NotNil(t, editOp, "edit operation should exist")
	require.Equal(t, editPerm, editOp.Parent(), "root operation's parent should be the permission")

	// Verify that child operations have correct parent
	if intersectionOp, ok := editOp.(*IntersectionOperation); ok {
		for _, child := range intersectionOp.Children() {
			require.Equal(t, intersectionOp, child.Parent(), "child operation should have intersection as parent")
		}
	}
}

// TestCrossPermissionReferences verifies that when one permission references another,
// the referencing operation has its parent set correctly.
func TestCrossPermissionReferences(t *testing.T) {
	// Build a schema where one permission references another
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").AllowedDirectRelation("user").Done().
		AddPermission("base_view").RelationRef("viewer").Done().
		AddPermission("extended_view").Union(
		&RelationReference{relationName: "base_view"}, // References another permission
		&RelationReference{relationName: "viewer"},
	).Done().
		Done().
		Build()

	def, found := schema.GetTypeDefinition("document")
	require.True(t, found, "definition should exist")

	baseViewPerm, found := def.GetPermission("base_view")
	require.True(t, found, "base_view permission should exist")

	extendedViewPerm, found := def.GetPermission("extended_view")
	require.True(t, found, "extended_view permission should exist")

	// Verify that base_view's operation belongs to base_view
	baseViewOp := baseViewPerm.Operation()
	require.NotNil(t, baseViewOp, "base_view operation should exist")
	require.Equal(t, baseViewPerm, baseViewOp.Parent(), "base_view root operation's parent should be base_view permission")

	// Verify that extended_view's operations belong to extended_view
	extendedViewOp := extendedViewPerm.Operation()
	require.NotNil(t, extendedViewOp, "extended_view operation should exist")
	require.Equal(t, extendedViewPerm, extendedViewOp.Parent(), "extended_view root operation's parent should be extended_view permission")

	// Verify that the child operation that references base_view has correct parent
	if unionOp, ok := extendedViewOp.(*UnionOperation); ok {
		for _, child := range unionOp.Children() {
			require.Equal(t, unionOp, child.Parent(), "child should have union as parent")
		}
	}
}

func TestOperationParentPointersFromProto(t *testing.T) {
	// Test that operations created from proto definitions also have their parent set correctly
	def := &corev1.NamespaceDefinition{
		Name: "document",
		Relation: []*corev1.Relation{
			{
				Name: "viewer",
				TypeInformation: &corev1.TypeInformation{
					AllowedDirectRelations: []*corev1.AllowedRelation{
						{
							Namespace: "user",
						},
					},
				},
			},
			{
				Name: "view",
				UsersetRewrite: &corev1.UsersetRewrite{
					RewriteOperation: &corev1.UsersetRewrite_Union{
						Union: &corev1.SetOperation{
							Child: []*corev1.SetOperation_Child{
								{
									ChildType: &corev1.SetOperation_Child_ComputedUserset{
										ComputedUserset: &corev1.ComputedUserset{
											Relation: "viewer",
										},
									},
								},
								{
									ChildType: &corev1.SetOperation_Child_TupleToUserset{
										TupleToUserset: &corev1.TupleToUserset{
											Tupleset: &corev1.TupleToUserset_Tupleset{
												Relation: "parent",
											},
											ComputedUserset: &corev1.ComputedUserset{
												Relation: "view",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	convertedDef, err := convertDefinition(def)
	require.NoError(t, err, "conversion should succeed")

	viewPerm, found := convertedDef.GetPermission("view")
	require.True(t, found, "view permission should exist")
	require.NotNil(t, viewPerm, "view permission should not be nil")

	// Verify that the operation has correct parent
	viewOp := viewPerm.Operation()
	require.NotNil(t, viewOp, "view operation should exist")
	require.Equal(t, viewPerm, viewOp.Parent(), "view operation's parent should be the permission")

	// Verify that child operations have correct parent
	if unionOp, ok := viewOp.(*UnionOperation); ok {
		for _, child := range unionOp.Children() {
			require.Equal(t, unionOp, child.Parent(), "child operation should have union as parent")
		}
	}
}
