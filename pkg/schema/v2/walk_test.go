package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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
		Definitions: make(map[string]*Definition),
		Caveats:     make(map[string]*Caveat),
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
		Definitions: map[string]*Definition{
			"user": {
				Name:        "user",
				Relations:   make(map[string]*Relation),
				Permissions: make(map[string]*Permission),
			},
			"document": {
				Name:        "document",
				Relations:   make(map[string]*Relation),
				Permissions: make(map[string]*Permission),
			},
		},
		Caveats: map[string]*Caveat{
			"is_admin": {
				Name:       "is_admin",
				Expression: "admin == true",
			},
		},
	}
	schema.Definitions["user"].Parent = schema
	schema.Definitions["document"].Parent = schema
	schema.Caveats["is_admin"].Parent = schema

	visitor := &testVisitor{}
	_, err := WalkSchema(schema, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.schemas, 1)
	require.Len(t, visitor.definitions, 2)
	require.Len(t, visitor.caveats, 1)
}

func TestWalkDefinition_WithRelations(t *testing.T) {
	rel1 := &Relation{
		Name:          "viewer",
		BaseRelations: []*BaseRelation{},
	}
	rel2 := &Relation{
		Name:          "editor",
		BaseRelations: []*BaseRelation{},
	}

	def := &Definition{
		Name: "document",
		Relations: map[string]*Relation{
			"viewer": rel1,
			"editor": rel2,
		},
		Permissions: make(map[string]*Permission),
	}
	rel1.Parent = def
	rel2.Parent = def

	visitor := &testVisitor{}
	_, err := WalkDefinition(def, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.definitions, 1)
	require.Len(t, visitor.relations, 2)
}

func TestWalkRelation_WithBaseRelations(t *testing.T) {
	br1 := &BaseRelation{
		Type: "user",
	}
	br2 := &BaseRelation{
		Type:        "user",
		Subrelation: "viewer",
	}
	br3 := &BaseRelation{
		Type:     "user",
		Wildcard: true,
	}

	rel := &Relation{
		Name:          "viewer",
		BaseRelations: []*BaseRelation{br1, br2, br3},
	}
	br1.Parent = rel
	br2.Parent = rel
	br3.Parent = rel

	visitor := &testVisitor{}
	_, err := WalkRelation(rel, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.relations, 1)
	require.Len(t, visitor.baseRelations, 3)
}

func TestWalkPermission_WithSimpleOperation(t *testing.T) {
	op := &RelationReference{
		RelationName: "viewer",
	}

	perm := &Permission{
		Name:      "view",
		Operation: op,
	}

	visitor := &testVisitor{}
	_, err := WalkPermission(perm, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.permissions, 1)
	require.Len(t, visitor.operations, 1)
	require.Len(t, visitor.relationReferences, 1)
	require.Equal(t, "viewer", visitor.relationReferences[0].RelationName)
}

func TestWalkOperation_RelationReference(t *testing.T) {
	op := &RelationReference{
		RelationName: "viewer",
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 1)
	require.Len(t, visitor.relationReferences, 1)
	require.Equal(t, "viewer", visitor.relationReferences[0].RelationName)
}

func TestWalkOperation_ArrowReference(t *testing.T) {
	op := &ArrowReference{
		Left:  "parent",
		Right: "viewer",
	}

	visitor := &testVisitor{}
	_, err := WalkOperation(op, visitor, struct{}{})
	require.NoError(t, err)

	require.Len(t, visitor.operations, 1)
	require.Len(t, visitor.arrowReferences, 1)
	require.Equal(t, "parent", visitor.arrowReferences[0].Left)
	require.Equal(t, "viewer", visitor.arrowReferences[0].Right)
}

func TestWalkOperation_UnionOperation(t *testing.T) {
	op := &UnionOperation{
		Children: []Operation{
			&RelationReference{RelationName: "viewer"},
			&RelationReference{RelationName: "editor"},
			&RelationReference{RelationName: "admin"},
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
		Children: []Operation{
			&RelationReference{RelationName: "viewer"},
			&RelationReference{RelationName: "approved"},
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
		Left:  &RelationReference{RelationName: "viewer"},
		Right: &RelationReference{RelationName: "banned"},
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
		Left: &IntersectionOperation{
			Children: []Operation{
				&UnionOperation{
					Children: []Operation{
						&RelationReference{RelationName: "viewer"},
						&RelationReference{RelationName: "editor"},
					},
				},
				&RelationReference{RelationName: "approved"},
			},
		},
		Right: &RelationReference{RelationName: "banned"},
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
		Children: []Operation{
			&ArrowReference{
				Left:  "parent",
				Right: "viewer",
			},
			&RelationReference{RelationName: "editor"},
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
		Type: "user",
	}
	rel1 := &Relation{
		Name:          "viewer",
		BaseRelations: []*BaseRelation{br1},
	}
	br1.Parent = rel1

	perm1 := &Permission{
		Name: "view",
		Operation: &UnionOperation{
			Children: []Operation{
				&RelationReference{RelationName: "viewer"},
				&ArrowReference{Left: "parent", Right: "view"},
			},
		},
	}

	def1 := &Definition{
		Name: "document",
		Relations: map[string]*Relation{
			"viewer": rel1,
		},
		Permissions: map[string]*Permission{
			"view": perm1,
		},
	}
	rel1.Parent = def1
	perm1.Parent = def1

	caveat1 := &Caveat{
		Name:       "is_admin",
		Expression: "admin == true",
	}

	schema := &Schema{
		Definitions: map[string]*Definition{
			"document": def1,
		},
		Caveats: map[string]*Caveat{
			"is_admin": caveat1,
		},
	}
	def1.Parent = schema
	caveat1.Parent = schema

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
		Definitions: map[string]*Definition{
			"user": {
				Name:        "user",
				Relations:   map[string]*Relation{},
				Permissions: map[string]*Permission{},
			},
		},
		Caveats: make(map[string]*Caveat),
	}
	schema.Definitions["user"].Parent = schema

	_, err := WalkSchema(schema, pv, struct{}{})
	require.NoError(t, err)

	// The walk should complete without panic even though visitor doesn't implement all interfaces
	// Should have visited the schema and definition
	require.Len(t, pv.schemas, 1)
	require.Len(t, pv.definitions, 1)
}

type errorVisitor struct{}

func (ev *errorVisitor) VisitDefinition(d *Definition, value struct{}) (struct{}, bool, error) {
	if d.Name == "document" {
		return value, false, errTestError
	}
	return value, true, nil
}

func TestWalkSchema_ErrorPropagation(t *testing.T) {
	schema := &Schema{
		Definitions: map[string]*Definition{
			"user": {
				Name:        "user",
				Relations:   make(map[string]*Relation),
				Permissions: make(map[string]*Permission),
			},
			"document": {
				Name:        "document",
				Relations:   make(map[string]*Relation),
				Permissions: make(map[string]*Permission),
			},
		},
		Caveats: make(map[string]*Caveat),
	}
	schema.Definitions["user"].Parent = schema
	schema.Definitions["document"].Parent = schema

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
		Definitions: map[string]*Definition{
			"user": {
				Name:        "user",
				Relations:   make(map[string]*Relation),
				Permissions: make(map[string]*Permission),
			},
		},
		Caveats: make(map[string]*Caveat),
	}
	schema.Definitions["user"].Parent = schema

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
		Name:          "viewer",
		BaseRelations: []*BaseRelation{},
	}
	def := &Definition{
		Name: "document",
		Relations: map[string]*Relation{
			"viewer": rel,
		},
		Permissions: make(map[string]*Permission),
	}
	rel.Parent = def

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
		Children: []Operation{
			&RelationReference{RelationName: "viewer"},
			&RelationReference{RelationName: "editor"},
			&RelationReference{RelationName: "admin"},
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
		Children: []Operation{
			&RelationReference{RelationName: "viewer"},
			&RelationReference{RelationName: "editor"},
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
		Children: []Operation{
			&RelationReference{RelationName: "viewer"},
			&RelationReference{RelationName: "editor"},
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
		Definitions: map[string]*Definition{
			"user": {
				Name:        "user",
				Relations:   make(map[string]*Relation),
				Permissions: make(map[string]*Permission),
			},
		},
		Caveats: make(map[string]*Caveat),
	}
	schema.Definitions["user"].Parent = schema

	visitor := &intVisitor{}
	result, err := WalkSchema(schema, visitor, 0)
	require.NoError(t, err)

	// Should have visited: Schema (+1), Definition (+10) = 11
	require.Equal(t, 11, result)
}

func TestWalkDefinition_ValueThreading(t *testing.T) {
	rel := &Relation{
		Name:          "viewer",
		BaseRelations: []*BaseRelation{{Type: "user"}},
	}
	perm := &Permission{
		Name:      "view",
		Operation: &RelationReference{RelationName: "viewer"},
	}

	def := &Definition{
		Name: "document",
		Relations: map[string]*Relation{
			"viewer": rel,
		},
		Permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.Parent = def
	rel.BaseRelations[0].Parent = rel
	perm.Parent = def

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
		Children: []Operation{
			&UnionOperation{
				Children: []Operation{
					&RelationReference{RelationName: "viewer"},
					&RelationReference{RelationName: "editor"},
				},
			},
			&RelationReference{RelationName: "approved"},
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
		Left:  &RelationReference{RelationName: "viewer"},
		Right: &RelationReference{RelationName: "banned"},
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
