package reachabilitypruning_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query/queryopt/reachabilitypruning"
)

func TestSemanticEquivalence(t *testing.T) {
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: reachabilitypruning.New(),

		Cases: map[string]func(*rapid.T) queryopttest.Case{
			"types-and-intermediate-hops": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `definition user {}
definition group {
 relation member: user
}
definition document {
 relation viewer: user | group
 relation parent: group
 permission view = viewer + parent->member
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@user:o_a", "document:o_a#parent@group:o_a", "group:o_a#member@user:o_b", "document:o_b#viewer@group:o_a")
				c.Requests = queryopttest.Requests("document", "view", "user")
				c.RequireChange = true

				return c
			},
			"named-subjects": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `definition user {}
definition group {
 relation member: user
}
definition document {
 relation viewer: group#member
 permission view = viewer
}`)
				c.Relationships = queryopttest.Relationships(t, "document:o_a#viewer@group:o_a#member")
				c.Requests = queryopttest.Requests("document", "view", "group")
				c.RequireChange = false
				for i := range c.Requests {
					c.Requests[i].Params.SubjectRelation = "member"
					c.Requests[i].Subject.Relation = "member"
				}
				return c
			},
		},
	})
}
