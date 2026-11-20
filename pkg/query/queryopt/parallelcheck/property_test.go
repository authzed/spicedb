package parallelcheck_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt/parallelcheck"
)

func TestSemanticEquivalence(t *testing.T) {
	pass, err := parallelcheck.New(4)
	require.NoError(t, err)
	config := queryopttest.Config{
		Optimizer: pass,
		Cases: map[string]func(*rapid.T) queryopttest.Case{
			"expiration": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `use expiration
definition user {}
definition document {
 relation reader: user with expiration
 relation writer: user
 permission view = reader & writer
}`)
				c.Relationships = queryopttest.Relationships(
					t,
					"document:o_a#reader@user:o_a",
					"document:o_a#writer@user:o_a",
				)
				expires := time.Now().Add(time.Hour)

				for index := range c.Relationships {
					if c.Relationships[index].Resource.Relation == "reader" {
						c.Relationships[index].OptionalExpiration = &expires
					}
				}

				c.Requests = queryopttest.Requests("document", "view", "user")
				return c
			},

			"nested-recursive-arrow": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `
definition user {}
definition group {
 relation member: user | group#member
 relation owner: user
 permission access = member + owner
}
definition document {
 relation parent: group
 relation reader: user
 relation writer: user
 permission local = reader & writer
 permission view = local + parent->access
}`)
				c.Relationships = queryopttest.Relationships(
					t,
					"document:o_a#reader@user:o_a",
					"document:o_a#writer@user:o_a",
					"document:o_b#parent@group:o_a",
					"group:o_a#member@group:o_b#member",
					"group:o_b#member@user:o_b",
				)
				c.Requests = queryopttest.Requests("document", "view", "user")
				return c
			},
			"repeated-subtree": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `
definition user {}
definition document {
 relation reader: user
 relation writer: user
 permission inner = reader + writer
 permission view = inner & (inner + reader)
}`)
				c.Relationships = queryopttest.Relationships(
					t,
					"document:o_a#reader@user:o_a",
					"document:o_b#writer@user:o_b",
				)
				c.Requests = queryopttest.Requests("document", "view", "user")
				return c
			},

			"conditional-composition": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `
caveat enabled(ok bool) { ok }
definition user {}
definition document {
 relation reader: user:* | user with enabled
 relation writer: user
 relation banned: user with enabled
 permission inner = reader & writer
 permission view = (inner + reader) - banned
}`)
				c.Relationships = queryopttest.Relationships(
					t,
					"document:o_a#reader@user:o_a[enabled]",
					"document:o_b#reader@user:*",
					"document:o_a#writer@user:o_a",
					"document:o_b#writer@user:o_b",
					"document:o_b#banned@user:o_a[enabled]",
				)
				c.Requests = queryopttest.Requests("document", "view", "user")

				for index := range c.Requests {
					c.Requests[index].CaveatContexts = []map[string]any{nil, {"ok": true}, {"ok": false}}
				}

				return c
			},
		},
	}

	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			config.ContextOptions = []query.ContextOption{
				query.WithCheckConcurrency(4),
				query.WithCheckExecution(query.CheckExecutionOptions{BatchChecks: batch}),
			}
			queryopttest.Check(t, config)
		})
	}
}
