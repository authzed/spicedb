package testing

import (
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// Empty generates a permission whose expression is nil.
//
// Example composition:
//
//	scenario := DrawScenario(t, Empty())
//
// Generated schema for this composition:
//
//	definition user {}
//	definition resource {
//		permission perm_0 = nil
//		permission view = perm_0
//	}
//
// This composition generates no relationships or update batches.
func Empty() *Shape { return &Shape{kind: emptyShape} }

func compileEmpty() schema.Operation { return &schema.NilReference{} }
