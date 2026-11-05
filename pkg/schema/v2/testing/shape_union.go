package testing

import (
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// Union combines one or more shapes by union on the same resource objects.
//
// Example composition:
//
//	scenario := DrawScenario(t, Union(Direct(), Direct()))
//
// Generated schema for this composition:
//
//	definition user {}
//	definition resource {
//		relation rel_0: user
//		relation rel_1: user
//		permission perm_2 = rel_0 + rel_1
//		permission view = perm_2
//	}
//
// Representative initial relationships (additional objects and memberships vary):
//
//	resource:obj_0#rel_0@user:shared
//	resource:obj_0#rel_0@user:left
//	resource:obj_0#rel_1@user:shared
//	resource:obj_0#rel_1@user:right
func Union(children ...*Shape) *Shape { return mustNewShape(unionShape, children...) }

func (b *scenarioBuilder) compileUnion(shape *Shape, definition string) schema.Operation {
	children := b.compileChildren(shape, definition)
	return schema.Union(children...)
}
