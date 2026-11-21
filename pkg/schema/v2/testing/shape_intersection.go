package testing

import (
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// Intersection requires all of one or more shapes on the same resource objects.
//
// Example composition:
//
//	scenario := DrawScenario(t, Intersection(Direct(), Direct()))
//
// Generated schema for this composition:
//
//	definition user {}
//	definition resource {
//		relation rel_0: user
//		relation rel_1: user
//		permission perm_2 = rel_0 & rel_1
//		permission view = perm_2
//	}
//
// Representative initial relationships (additional objects and memberships vary):
//
//	resource:obj_0#rel_0@user:shared
//	resource:obj_0#rel_0@user:left
//	resource:obj_0#rel_1@user:shared
//	resource:obj_0#rel_1@user:right
func Intersection(children ...*Shape) *Shape { return mustNewShape(intersectionShape, children...) }

func (b *scenarioBuilder) compileIntersection(shape *Shape, definition string) schema.Operation {
	children := b.compileChildren(shape, definition)
	return schema.Intersection(children...)
}
