package testing

import (
	"pgregory.net/rapid"
)

// Direct generates a relation with direct user membership. Each call creates an
// independent relation; reuse the returned value to share a relation.
//
// Example composition:
//
//	scenario := DrawScenario(t, Direct())
//
// Generated schema for this composition:
//
//	definition user {}
//	definition resource {
//		relation rel_0: user
//		permission view = rel_0
//	}
//
// Representative initial relationships (additional objects and memberships vary):
//
//	resource:obj_0#rel_0@user:shared
//	resource:obj_0#rel_0@user:left
//	resource:obj_1#rel_0@user:shared
//	resource:obj_1#rel_0@user:right
func Direct() *Shape { return &Shape{kind: directShape} }

func (b *scenarioBuilder) compileDirect(definition string) string {
	def := b.builder.AddDefinition(definition)
	relation := b.name("rel")
	def.AddRelation(relation).AllowedDirectRelation("user")
	parity := b.nextDirect % 2
	b.nextDirect++
	for i := range b.objects {
		for j, subject := range b.subjects {
			present := false
			switch j {
			case 0:
				present = true
			case 1, 2:
				present = (i+parity)%2 == j-1
			case 3:
				// Keep a concrete negative check candidate in the initial graph.
			default:
				present = rapid.Bool().Draw(b.t, definition+"/"+relation+"/member")
			}
			b.relationship(object(definition, i, relation), subject, present)
		}
	}
	return relation
}
