package aliaschaincollapse_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt/aliaschaincollapse"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
)

func TestSemanticEquivalence(t *testing.T) {
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: aliaschaincollapse.New(),
		Applicable: func(p optimization.RequestParams) bool {
			return p.Operation == query.OperationCheck || p.SubjectRelation == "..."
		},
		Cases: map[string]func(*rapid.T) queryopttest.Case{
			"alias-chain": func(t *rapid.T) queryopttest.Case {
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
			"alias-self-edges": func(t *rapid.T) queryopttest.Case {
				c := queryopttest.ParseCase(t, `definition user {}
definition group {
 relation member: user
 permission inner = member
 permission view = inner
}`)
				c.Relationships = queryopttest.Relationships(t, "group:o_a#member@user:o_a", "group:o_b#member@user:o_b")
				c.Requests = queryopttest.Requests("group", "view", "user")
				c.RequireChange = true
				for _, relation := range []string{"member", "inner", "view"} {
					c.Requests = append(c.Requests, queryopttest.Request{Params: optimization.RequestParams{Operation: query.OperationCheck, SubjectType: "group", SubjectRelation: relation}, Resource: query.NewObject("group", "o_a"), Permission: "view", Subject: query.NewObject("group", "o_a").WithRelation(relation)})
				}
				return c
			},
		},
	})
}
