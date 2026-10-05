package setsimplification_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query/queryopt/setsimplification"
)

func TestSemanticEquivalence(t *testing.T) {
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: setsimplification.New(),

		Cases: map[string]func(*rapid.T) queryopttest.Case{
			"absorption": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `definition user {}
definition document {
 relation viewer: user
 relation editor: user
 permission view = viewer + (viewer & editor)
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:o_a", "document:o_b#viewer@user:o_b", "document:o_a#editor@user:o_a")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = true

				return c
			},
			"wildcard-exclusion": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `definition user {}
definition document {
 relation viewer: user:*
 relation banned: user
 permission base = viewer - banned
 permission view = base + (base & banned)
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:*", "document:o_a#banned@user:o_a", "document:o_b#viewer@user:*", "document:o_b#banned@user:o_b")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = true

				return c
			},
			"conditional-wildcard-exclusion": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `caveat enabled(ok bool) { ok }
definition user {}
definition document {
 relation viewer: user:*
 relation banned: user with enabled
 permission base = viewer - banned
 permission view = base + (base & banned)
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:*", "document:o_a#banned@user:o_a[enabled]", "document:o_b#viewer@user:*", "document:o_b#banned@user:o_b[enabled]")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = true
				for i := range c.Requests {
					c.Requests[i].CaveatContexts = []map[string]any{nil, {"ok": true}, {"ok": false}}
				}

				return c
			},
			"caveated-absorption": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `caveat enabled(ok bool) { ok }
definition user {}
definition document {
 relation viewer: user with enabled
 relation editor: user
 permission view = viewer + (viewer & editor)
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:o_a[enabled]", "document:o_b#viewer@user:o_b[enabled]", "document:o_a#editor@user:o_a")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = true
				for i := range c.Requests {
					c.Requests[i].CaveatContexts = []map[string]any{nil, {"ok": true}, {"ok": false}}
				}
				return c
			},
		},
	})
}
