package membership

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/graph"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	ONR      = tuple.ObjectAndRelation
	Ellipsis = "..."
)

var (
	_this *v0.ObjectAndRelation

	companyOwner = graph.Leaf(ONR("folder", "company", "owner"),
		tuple.User(ONR("user", "owner", Ellipsis)),
	)
	companyEditor = graph.Union(ONR("folder", "company", "editor"),
		graph.Leaf(_this, tuple.User(ONR("user", "writer", Ellipsis))),
		companyOwner,
	)

	auditorsOwner = graph.Leaf(ONR("folder", "auditors", "owner"))

	auditorsEditor = graph.Union(ONR("folder", "auditors", "editor"),
		graph.Leaf(_this),
		auditorsOwner,
	)

	auditorsViewerRecursive = graph.Union(ONR("folder", "auditors", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "auditor", "...")),
		),
		auditorsEditor,
		graph.Union(ONR("folder", "auditors", "viewer")),
	)

	companyViewerRecursive = graph.Union(ONR("folder", "company", "viewer"),
		graph.Union(ONR("folder", "company", "viewer"),
			auditorsViewerRecursive,
			graph.Leaf(_this,
				tuple.User(ONR("user", "legal", "...")),
				tuple.User(ONR("folder", "auditors", "viewer")),
			),
		),
		companyEditor,
		graph.Union(ONR("folder", "company", "viewer")),
	)
)

func TestMembershipSet(t *testing.T) {
	require := require.New(t)
	ms := NewMembershipSet()

	// Add some expansion trees.
	fso, ok := ms.AddExpansion(ONR("folder", "company", "owner"), companyOwner)
	require.True(ok)
	verifySubjects(require, fso, "user:owner#...")

	fse, ok := ms.AddExpansion(ONR("folder", "company", "editor"), companyEditor)
	require.True(ok)
	verifySubjects(require, fse, "user:owner#...", "user:writer#...")

	fsv, ok := ms.AddExpansion(ONR("folder", "company", "viewer"), companyViewerRecursive)
	require.True(ok)
	verifySubjects(require, fsv, "folder:auditors#viewer", "user:auditor#...", "user:legal#...", "user:owner#...", "user:writer#...")
}

func verifySubjects(require *require.Assertions, fs FoundSubjects, expected ...string) {
	foundSubjects := []*v0.ObjectAndRelation{}
	for _, found := range fs.ListFound() {
		foundSubjects = append(foundSubjects, found.Subject())

		_, ok := fs.LookupSubject(found.Subject())
		require.True(ok)
	}

	require.Equal(expected, tuple.StringsONRs(foundSubjects))
}
