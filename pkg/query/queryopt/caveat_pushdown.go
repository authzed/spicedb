package queryopt

import "github.com/authzed/spicedb/pkg/query"

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "simple-caveat-pushdown",
		Description: `
		Pushes caveat evalution to the lowest point in the tree.
		Cannot push through intersection arrows
		`,
		Mutation: caveatPushdown,
	})
}

func caveatPushdown(outline query.Outline) query.Outline {
	// CLAUDE
}
