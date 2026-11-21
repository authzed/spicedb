package testing

import (
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// Arrow generates a relation to intermediate objects and follows an ordinary
// (any) arrow to the child on those objects.
//
// Example composition:
//
//	scenario := DrawScenario(t, Arrow(Direct()))
//
// Generated schema for this composition:
//
//	definition user {}
//	definition type_0 {
//		relation rel_1: user
//	}
//	definition resource {
//		relation rel_2: type_0
//		permission perm_3 = rel_2->rel_1
//		permission view = perm_3
//	}
//
// Representative initial relationships (additional objects and memberships vary):
//
//	resource:obj_1#rel_2@type_0:obj_0
//	resource:obj_1#rel_2@type_0:obj_1
//	type_0:obj_0#rel_1@user:shared
//	type_0:obj_0#rel_1@user:left
//	type_0:obj_1#rel_1@user:shared
//	type_0:obj_1#rel_1@user:right
func Arrow(child *Shape) *Shape { return mustNewShape(arrowShape, child) }

func (b *scenarioBuilder) compileArrow(shape *Shape, definition string) schema.Operation {
	relation, ref := b.compileLinked(shape.children[0], definition, false)
	return schema.NewArrow(relation, ref)
}
