package testing

import (
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// Userset generates a relation to objects with the child's relation/permission
// as their subject relation, including the backing memberships on those objects.
//
// Example composition:
//
//	scenario := DrawScenario(t, Userset(Direct()))
//
// Generated schema for this composition:
//
//	definition user {}
//	definition type_0 {
//		relation rel_1: user
//	}
//	definition resource {
//		relation rel_2: type_0#rel_1
//		permission perm_3 = rel_2
//		permission view = perm_3
//	}
//
// Representative initial relationships (additional objects and memberships vary):
//
//	resource:obj_1#rel_2@type_0:obj_0#rel_1
//	resource:obj_1#rel_2@type_0:obj_1#rel_1
//	type_0:obj_0#rel_1@user:shared
//	type_0:obj_0#rel_1@user:left
//	type_0:obj_1#rel_1@user:shared
//	type_0:obj_1#rel_1@user:right
func Userset(child *Shape) *Shape { return mustNewShape(usersetShape, child) }

func (b *scenarioBuilder) compileUserset(shape *Shape, definition string) schema.Operation {
	relation, _ := b.compileLinked(shape.children[0], definition, true)
	return schema.NewRelationRef(relation)
}
