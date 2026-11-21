package testing

import (
	"fmt"

	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Scenario is a schema and a small, connected relationship graph, followed by
// batches to apply in order. Write Relationships before applying UpdateBatches;
// each batch is a separate transaction. No batch contains duplicate relationship
// identities, and CREATE only targets relationships absent at that point.
//
// Resources contains root permission check targets (including a missing object).
// Subjects contains concrete user check targets, including an initially absent
// user. Keep these candidates across revisions, even after deleting their last
// relationship, to test revocation as well as grants.
type Scenario struct {
	Schema        *schema.Schema
	Relationships []tuple.Relationship
	UpdateBatches [][]tuple.RelationshipUpdate
	Resources     []tuple.ObjectAndRelation
	Subjects      []tuple.ObjectAndRelation
}

// DrawScenario instantiates a shape using Rapid draws for graph size, additional
// membership, fan-out, and updates. Small shared object pools create overlap:
// direct relations include a common user, alternating left/right users, and
// optional extra users. Intermediate objects have differing memberships so any
// and all arrows can produce different answers.
//
// The first four update batches touch, remove, delete again, and recreate a
// selected set of initial relationships. This prefix restores the initial graph
// and exercises idempotency. Further random batches vary the graph. An entirely
// empty shape has no relationships or updates. All returned data belongs to this
// draw; generating another scenario does not modify it or the shape.
func DrawScenario(t *rapid.T, shape *Shape) Scenario {
	if shape == nil {
		t.Fatal("scenario shape must not be nil")
	}
	b := &scenarioBuilder{
		t:       t,
		builder: schema.NewSchemaBuilder(),
		objects: rapid.IntRange(2, 3).Draw(t, "objects"),
		refs:    map[shapeLocation]string{},
		nested:  map[*Shape]string{},
	}
	b.builder.AddDefinition("user")
	for _, id := range []string{"shared", "left", "right", "absent"} {
		b.subjects = append(b.subjects, tuple.ObjectAndRelation{ObjectType: "user", ObjectID: id, Relation: tuple.Ellipsis})
	}
	for i := range rapid.IntRange(0, 2).Draw(t, "extraSubjects") {
		b.subjects = append(b.subjects, tuple.ObjectAndRelation{ObjectType: "user", ObjectID: fmt.Sprintf("extra_%d", i), Relation: tuple.Ellipsis})
	}
	entry := b.compile(shape, "resource")
	b.builder.AddDefinition("resource").AddPermission("view").RelationRef(entry)
	result := Scenario{
		Schema:        b.builder.Build(),
		Relationships: b.relationships,
		Subjects:      b.subjects,
		UpdateBatches: drawUpdateBatches(t, b.relationships, b.candidates),
	}
	for i := range b.objects {
		result.Resources = append(result.Resources, object("resource", i, "view"))
	}
	result.Resources = append(result.Resources, tuple.ObjectAndRelation{ObjectType: "resource", ObjectID: "missing", Relation: "view"})
	return result
}

type shapeLocation struct {
	shape      *Shape
	definition string
}

type scenarioBuilder struct {
	t             *rapid.T
	builder       *schema.SchemaBuilder
	objects       int
	subjects      []tuple.ObjectAndRelation
	nextName      int
	nextDirect    int
	refs          map[shapeLocation]string
	nested        map[*Shape]string
	relationships []tuple.Relationship
	candidates    []tuple.Relationship
}

func (b *scenarioBuilder) name(prefix string) string {
	name := fmt.Sprintf("%s_%d", prefix, b.nextName)
	b.nextName++
	return name
}

func object(definition string, index int, relation string) tuple.ObjectAndRelation {
	return tuple.ObjectAndRelation{ObjectType: definition, ObjectID: fmt.Sprintf("obj_%d", index), Relation: relation}
}

func (b *scenarioBuilder) relationship(resource, subject tuple.ObjectAndRelation, present bool) {
	rel := tuple.Relationship{RelationshipReference: tuple.RelationshipReference{Resource: resource, Subject: subject}}
	b.candidates = append(b.candidates, rel)
	if present {
		b.relationships = append(b.relationships, rel)
	}
}

// compile returns a reference name instead of an Operation, so each use gets
// its own operation node and preserves the schema's parent pointers.
func (b *scenarioBuilder) compile(shape *Shape, definition string) string {
	key := shapeLocation{shape, definition}
	if ref, ok := b.refs[key]; ok {
		return ref
	}
	b.builder.AddDefinition(definition)
	var op schema.Operation
	switch shape.kind {
	case directShape:
		relation := b.compileDirect(definition)
		b.refs[key] = relation
		return relation
	case emptyShape:
		op = compileEmpty()
	case usersetShape:
		op = b.compileUserset(shape, definition)
	case arrowShape:
		op = b.compileArrow(shape, definition)
	case allArrowShape:
		op = b.compileAllArrow(shape, definition)
	case aliasShape:
		op = b.compileAlias(shape, definition)
	case unionShape:
		op = b.compileUnion(shape, definition)
	case intersectionShape:
		op = b.compileIntersection(shape, definition)
	case exclusionShape:
		op = b.compileExclusion(shape, definition)
	default:
		panic("unsupported shape")
	}
	permission := b.name("perm")
	b.builder.AddDefinition(definition).AddPermission(permission).Operation(op)
	b.refs[key] = permission
	return permission
}

// compileChildren creates a fresh operation for each reference to preserve parent pointers.
func (b *scenarioBuilder) compileChildren(shape *Shape, definition string) []schema.Operation {
	children := make([]schema.Operation, 0, len(shape.children))
	for _, child := range shape.children {
		children = append(children, schema.NewRelationRef(b.compile(child, definition)))
	}
	return children
}

// compileLinked generates the shared backing graph for usersets and arrows.
func (b *scenarioBuilder) compileLinked(child *Shape, definition string, userset bool) (string, string) {
	def := b.builder.AddDefinition(definition)
	target, ok := b.nested[child]
	if !ok {
		target = b.name("type")
		b.nested[child] = target
	}
	ref := b.compile(child, target)
	relation := b.name("rel")
	subjectRelation := tuple.Ellipsis
	if userset {
		def.AddRelation(relation).AllowedRelation(target, ref)
		subjectRelation = ref
	} else {
		def.AddRelation(relation).AllowedDirectRelation(target)
	}
	for i := range b.objects {
		for j := range b.objects {
			// Include a common target and the matching object; additional
			// edges vary fan-out while retaining connected paths as we shrink.
			present := j == 0 || i == j || rapid.Bool().Draw(b.t, definition+"/"+relation+"/link")
			b.relationship(object(definition, i, relation), object(target, j, subjectRelation), present)
		}
	}
	return relation, ref
}
