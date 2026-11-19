package caveatpushdown_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query/queryopt/caveatpushdown"
)

func TestSemanticEquivalence(t *testing.T) {
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: caveatpushdown.New(),

		Cases: map[string]func(*rapid.T) queryopttest.Case{
			"mixed-caveats": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `caveat enabled(ok bool) { ok }
definition user {}
definition document {
 relation viewer: user | user with enabled
 relation editor: user
 permission view = viewer + editor
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:o_a[enabled]", "document:o_b#viewer@user:o_b", "document:o_a#editor@user:o_b")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = true
				for i := range c.Requests {
					c.Requests[i].CaveatContexts = []map[string]any{nil, {"ok": true}, {"ok": false}}
				}
				return c
			},
			"multiple-caveats-and-all": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `caveat enabled(ok bool) { ok }
caveat permitted(permit bool) { permit }
definition user {}
definition group {
 relation member: user with enabled | user with permitted
}
definition document {
 relation parent: group
 relation viewer: user with enabled
 permission view = viewer + parent.all(member)
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:o_a[enabled]", "document:o_a#parent@group:o_a", "group:o_a#member@user:o_a[permitted]")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = false
				for i := range c.Requests {
					c.Requests[i].CaveatContexts = []map[string]any{nil, {"ok": true, "permit": true}, {"ok": false, "permit": true}, {"ok": true}, {"permit": false}}
				}
				return c
			},
		},
	})
}
