package dispatchwrap_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/queryopt/dispatchwrap"
	"github.com/authzed/spicedb/internal/queryopttest"
)

func TestSemanticEquivalence(t *testing.T) {
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: dispatchwrap.New(dispatch.DispatchIteratorType),

		Cases: map[string]func(*rapid.T) queryopttest.Case{
			"nested-aliases": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `definition user {}
definition document {
 relation viewer: user
 permission read = viewer
 permission view = read
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:o_a", "document:o_b#viewer@user:o_b")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = true

				return c
			},
			"recursive-boundaries": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `definition user {}
definition group {
 relation direct: user
 relation parent: group
 permission member = direct + parent->member
 permission view = member
}`)
				c.Relationships = queryopttest.Relationships(t, "group:o_a#direct@user:o_a", "group:o_b#parent@group:o_a", "group:o_a#parent@group:o_b")
				c.Requests = queryopttest.Requests("group", "view", "user")
				c.RequireChange = true

				return c
			},
		},
	})
}
