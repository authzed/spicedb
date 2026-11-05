package testing

import (
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// Alias adds a permission that references the child's relation or permission.
//
// Example composition:
//
//	scenario := DrawScenario(t, Alias(Direct()))
//
// Generated schema for this composition:
//
//	definition user {}
//	definition resource {
//		relation rel_0: user
//		permission perm_1 = rel_0
//		permission view = perm_1
//	}
//
// Representative initial relationships (additional objects and memberships vary):
//
//	resource:obj_0#rel_0@user:shared
//	resource:obj_0#rel_0@user:left
func Alias(child *Shape) *Shape { return mustNewShape(aliasShape, child) }

func (b *scenarioBuilder) compileAlias(shape *Shape, definition string) schema.Operation {
	return b.compileChildren(shape, definition)[0]
}
