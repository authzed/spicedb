package graph

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/graph/hints"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	ONR = tuple.ONR
	RR  = tuple.RR
)

func TestSimpleCheck(t *testing.T) {
	t.Parallel()

	type expected struct {
		relation string
		isMember bool
	}

	type userset struct {
		userset  tuple.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", true}, {"edit", true}, {"view", true}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
		}},
		{"document", "healthplan", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
		}},
		{"folder", "company", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", true}, {"edit", true}, {"view", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("folder", "auditors", "viewer"), []expected{{"view", true}}},
		}},
		{"folder", "strategy", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", true}, {"edit", true}, {"view", true}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("folder", "company", graph.Ellipsis), []expected{{"parent", true}}},
		}},
		{"folder", "isolated", []userset{
			{ONR("user", "product_manager", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "chief_financial_officer", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "owner", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "legal", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "vp_product", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "eng_lead", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "auditor", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", false}}},
			{ONR("user", "villain", graph.Ellipsis), []expected{{"owner", false}, {"edit", false}, {"view", true}}},
		}},
	}

	for _, tc := range testCases {
		for _, userset := range tc.usersets {
			for _, expected := range userset.expected {
				name := fmt.Sprintf(
					"simple::%s:%s#%s@%s:%s#%s=>%t",
					tc.namespace,
					tc.objectID,
					expected.relation,
					userset.userset.ObjectType,
					userset.userset.ObjectID,
					userset.userset.Relation,
					expected.isMember,
				)

				tc := tc
				userset := userset
				expected := expected
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					require := require.New(t)

					ctx, dispatch, revision := newLocalDispatcher(t)

					checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
						ResourceRelation: RR(tc.namespace, expected.relation).ToCoreRR(),
						ResourceIds:      []string{tc.objectID},
						ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
						Subject:          userset.userset.ToCoreONR(),
						Metadata: &v1.ResolverMeta{
							AtRevision:     revision.String(),
							DepthRemaining: 50,
						},
					})

					require.NoError(err)

					isMember := false
					if found, ok := checkResult.ResultsByResourceId[tc.objectID]; ok {
						isMember = found.Membership == v1.ResourceCheckResult_MEMBER
					}

					require.Equal(expected.isMember, isMember, "For object %s in %v: ", tc.objectID, checkResult.ResultsByResourceId)
					require.GreaterOrEqual(checkResult.Metadata.DepthRequired, uint32(1))
				})
			}
		}
	}
}

func TestMaxDepth(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	mutation := tuple.Create(tuple.MustParse("folder:oops#parent@folder:oops"))

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(datastoremw.SetInContext(ctx, ds))

	revision, err := common.UpdateRelationshipsInDatastore(ctx, ds, mutation)
	require.NoError(err)

	dispatch := NewLocalOnlyDispatcher(10, 100)

	_, err = dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: RR("folder", "view").ToCoreRR(),
		ResourceIds:      []string{"oops"},
		ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
		Subject:          tuple.CoreONR("user", "fake", graph.Ellipsis),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	})

	require.Error(err)
}

func TestCheckMetadata(t *testing.T) {
	t.Parallel()
	type expected struct {
		relation              string
		isMember              bool
		expectedDispatchCount int
		expectedDepthRequired int
	}

	type userset struct {
		userset  tuple.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{
				ONR("user", "product_manager", graph.Ellipsis),
				[]expected{
					{"owner", true, 1, 1},
					{"edit", true, 3, 2},
					{"view", true, 21, 5},
				},
			},
			{
				ONR("user", "owner", graph.Ellipsis),
				[]expected{
					{"owner", false, 1, 1},
					{"edit", false, 3, 2},
					{"view", true, 21, 5},
				},
			},
		}},
		{"folder", "strategy", []userset{
			{
				ONR("user", "vp_product", graph.Ellipsis),
				[]expected{
					{"owner", true, 1, 1},
					{"edit", true, 3, 2},
					{"view", true, 11, 4},
				},
			},
		}},
		{"folder", "company", []userset{
			{
				ONR("user", "unknown", graph.Ellipsis),
				[]expected{
					{"view", false, 6, 3},
				},
			},
		}},
	}

	for _, tc := range testCases {
		for _, userset := range tc.usersets {
			for _, expected := range userset.expected {
				name := fmt.Sprintf(
					"metadata:%s:%s#%s@%s:%s#%s=>%t",
					tc.namespace,
					tc.objectID,
					expected.relation,
					userset.userset.ObjectType,
					userset.userset.ObjectID,
					userset.userset.Relation,
					expected.isMember,
				)

				tc := tc
				userset := userset
				expected := expected
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					require := require.New(t)

					ctx, dispatch, revision := newLocalDispatcher(t)

					checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
						ResourceRelation: RR(tc.namespace, expected.relation).ToCoreRR(),
						ResourceIds:      []string{tc.objectID},
						ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
						Subject:          userset.userset.ToCoreONR(),
						Metadata: &v1.ResolverMeta{
							AtRevision:     revision.String(),
							DepthRemaining: 50,
						},
					})

					require.NoError(err)

					isMember := false
					if found, ok := checkResult.ResultsByResourceId[tc.objectID]; ok {
						isMember = found.Membership == v1.ResourceCheckResult_MEMBER
					}

					require.Equal(expected.isMember, isMember)
					require.GreaterOrEqual(expected.expectedDispatchCount, int(checkResult.Metadata.DispatchCount), "dispatch count mismatch")
					require.GreaterOrEqual(expected.expectedDepthRequired, int(checkResult.Metadata.DepthRequired), "depth required mismatch")
				})
			}
		}
	}
}

func TestCheckPermissionOverSchema(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                      string
		schema                    string
		relationships             []tuple.Relationship
		resource                  tuple.ObjectAndRelation
		subject                   tuple.ObjectAndRelation
		expectedPermissionship    v1.ResourceCheckResult_Membership
		expectedCaveat            *core.CaveatExpression
		alternativeExpectedCaveat *core.CaveatExpression
	}{
		{
			"basic union",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic intersection",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#editor@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic exclusion",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer - editor
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic union, multiple branches",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#editor@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic union no permission",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"basic intersection no permission",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"basic exclusion no permission",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#banned@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"exclusion with multiple branches",
			`definition user {}
		
			 definition group {
			 	relation member: user
				relation banned: user
			 	permission view = member - banned
			 }

		 	 definition document {
				relation group: group
				permission view = group->view
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#group@group:first"),
				tuple.MustParse("document:first#group@group:second"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:first#banned@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"intersection with multiple branches",
			`definition user {}
		
			 definition group {
			 	relation member: user
				relation other: user
			 	permission view = member & other
			 }

		 	 definition document {
				relation group: group
				permission view = group->view
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#group@group:first"),
				tuple.MustParse("document:first#group@group:second"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:first#other@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"exclusion with multiple branches no permission",
			`definition user {}
		
			 definition group {
			 	relation member: user
				relation banned: user
			 	permission view = member - banned
			 }

		 	 definition document {
				relation group: group
				permission view = group->view
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#group@group:first"),
				tuple.MustParse("document:first#group@group:second"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:first#banned@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
				tuple.MustParse("group:second#banned@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"intersection with multiple branches no permission",
			`definition user {}
		
			 definition group {
			 	relation member: user
				relation other: user
			 	permission view = member & other
			 }

		 	 definition document {
				relation group: group
				permission view = group->view
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#group@group:first"),
				tuple.MustParse("document:first#group@group:second"),
				tuple.MustParse("group:first#member@user:tom"),
				tuple.MustParse("group:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"basic arrow",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }
		
		 	 definition document {
				relation orgs: organization
				permission view = orgs->member
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("document:first#orgs@organization:second"),
				tuple.MustParse("organization:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic any arrow",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }
		
		 	 definition document {
				relation orgs: organization
				permission view = orgs.any(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("document:first#orgs@organization:second"),
				tuple.MustParse("organization:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic all arrow negative",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }
		
		 	 definition document {
				relation orgs: organization
				permission view = orgs.all(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("document:first#orgs@organization:second"),
				tuple.MustParse("organization:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"basic all arrow positive",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }
		
		 	 definition document {
				relation orgs: organization
				permission view = orgs.all(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("document:first#orgs@organization:second"),
				tuple.MustParse("organization:first#member@user:tom"),
				tuple.MustParse("organization:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic all arrow positive with different types",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }

			 definition someotherresource {
			  	relation member: user
  			}
		
		 	 definition document {
				relation orgs: organization | someotherresource
				permission view = orgs.all(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("document:first#orgs@organization:second"),
				tuple.MustParse("organization:first#member@user:tom"),
				tuple.MustParse("organization:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"basic all arrow negative over different types",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }

			 definition someotherresource {
			  	relation member: user
  			}
		
		 	 definition document {
				relation orgs: organization | someotherresource
				permission view = orgs.all(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("document:first#orgs@organization:second"),
				tuple.MustParse("document:first#orgs@someotherresource:other"),
				tuple.MustParse("organization:first#member@user:tom"),
				tuple.MustParse("organization:second#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"basic all arrow positive over different types",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }

			 definition someotherresource {
			  	relation member: user
  			}
		
		 	 definition document {
				relation orgs: organization | someotherresource
				permission view = orgs.all(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("document:first#orgs@organization:second"),
				tuple.MustParse("document:first#orgs@someotherresource:other"),
				tuple.MustParse("organization:first#member@user:tom"),
				tuple.MustParse("organization:second#member@user:tom"),
				tuple.MustParse("someotherresource:other#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"all arrow for single org",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }
		
		 	 definition document {
				relation orgs: organization
				permission view = orgs.all(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#orgs@organization:first"),
				tuple.MustParse("organization:first#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"all arrow for no orgs",
			`definition user {}

			 definition organization {
			 	relation member: user
			 }
		
		 	 definition document {
				relation orgs: organization
				permission view = orgs.all(member)
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("organization:first#member@user:tom"),
			},
			ONR("document", "first", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"view_by_all negative",
			`  definition user {}

  definition team {
    relation direct_member: user
    permission member = direct_member
  }

  definition resource {
    relation team: team
    permission view_by_all = team.all(member)
    permission view_by_any = team.any(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse("team:first#direct_member@user:tom"),
				tuple.MustParse("team:first#direct_member@user:fred"),
				tuple.MustParse("team:first#direct_member@user:sarah"),
				tuple.MustParse("team:second#direct_member@user:fred"),
				tuple.MustParse("team:second#direct_member@user:sarah"),
				tuple.MustParse("team:third#direct_member@user:sarah"),
				tuple.MustParse("resource:oneteam#team@team:first"),
				tuple.MustParse("resource:twoteams#team@team:first"),
				tuple.MustParse("resource:twoteams#team@team:second"),
				tuple.MustParse("resource:threeteams#team@team:first"),
				tuple.MustParse("resource:threeteams#team@team:second"),
				tuple.MustParse("resource:threeteams#team@team:third"),
			},
			ONR("resource", "threeteams", "view_by_all"),
			ONR("user", "fred", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"view_by_any positive",
			`  definition user {}

  definition team {
    relation direct_member: user
    permission member = direct_member
  }

  definition resource {
    relation team: team
	relation viewer: user
    permission view_by_all = team.all(member) + viewer
    permission view_by_any = team.any(member) + viewer
  }`,
			[]tuple.Relationship{
				tuple.MustParse("team:first#direct_member@user:tom"),
				tuple.MustParse("team:first#direct_member@user:fred"),
				tuple.MustParse("team:first#direct_member@user:sarah"),
				tuple.MustParse("team:second#direct_member@user:fred"),
				tuple.MustParse("team:second#direct_member@user:sarah"),
				tuple.MustParse("team:third#direct_member@user:sarah"),
				tuple.MustParse("resource:oneteam#team@team:first"),
				tuple.MustParse("resource:twoteams#team@team:first"),
				tuple.MustParse("resource:twoteams#team@team:second"),
				tuple.MustParse("resource:threeteams#team@team:first"),
				tuple.MustParse("resource:threeteams#team@team:second"),
				tuple.MustParse("resource:threeteams#team@team:third"),
				tuple.MustParse("resource:oneteam#viewer@user:rachel"),
			},
			ONR("resource", "threeteams", "view_by_any"),
			ONR("user", "fred", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"view_by_any positive directly",
			`  definition user {}

  definition team {
    relation direct_member: user
    permission member = direct_member
  }

  definition resource {
    relation team: team
	relation viewer: user
    permission view_by_all = team.all(member) + viewer
    permission view_by_any = team.any(member) + viewer
  }`,
			[]tuple.Relationship{
				tuple.MustParse("team:first#direct_member@user:tom"),
				tuple.MustParse("team:first#direct_member@user:fred"),
				tuple.MustParse("team:first#direct_member@user:sarah"),
				tuple.MustParse("team:second#direct_member@user:fred"),
				tuple.MustParse("team:second#direct_member@user:sarah"),
				tuple.MustParse("team:third#direct_member@user:sarah"),
				tuple.MustParse("resource:oneteam#team@team:first"),
				tuple.MustParse("resource:twoteams#team@team:first"),
				tuple.MustParse("resource:twoteams#team@team:second"),
				tuple.MustParse("resource:threeteams#team@team:first"),
				tuple.MustParse("resource:threeteams#team@team:second"),
				tuple.MustParse("resource:threeteams#team@team:third"),
				tuple.MustParse("resource:oneteam#viewer@user:rachel"),
			},
			ONR("resource", "oneteam", "view_by_any"),
			ONR("user", "rachel", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"caveated intersection arrow",
			`  definition user {}

  definition team {
    relation direct_member: user
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam == 42
  }

  definition resource {
    relation team: team with somecaveat
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse("team:first#direct_member@user:tom"),
				tuple.MustParse("resource:oneteam#team@team:first[somecaveat]"),
			},
			ONR("resource", "oneteam", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAndCtx("somecaveat", nil),
			nil,
		},
		{
			"intersection arrow with caveated member",
			`  definition user {}

  definition team {
    relation direct_member: user with somecaveat
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam == 42
  }

  definition resource {
    relation team: team
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse("team:first#direct_member@user:tom[somecaveat]"),
				tuple.MustParse("resource:oneteam#team@team:first"),
			},
			ONR("resource", "oneteam", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAndCtx("somecaveat", nil),
			nil,
		},
		{
			"caveated intersection arrow with caveated member",
			`  definition user {}

  definition team {
    relation direct_member: user with somecaveat
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam == 42
  }

  definition resource {
    relation team: team with somecaveat
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse("team:first#direct_member@user:tom[somecaveat]"),
				tuple.MustParse("resource:oneteam#team@team:first[somecaveat]"),
			},
			ONR("resource", "oneteam", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAndCtx("somecaveat", nil),
			nil,
		},
		{
			"caveated intersection arrow with caveated member, different context",
			`definition user {}

  definition team {
    relation direct_member: user with anothercaveat
    permission member = direct_member
  }

  caveat anothercaveat(someparam int) {
    someparam == 43
  }

  caveat somecaveat(someparam int) {
    someparam == 42
  }

  definition resource {
    relation team: team with somecaveat
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse(`team:first#direct_member@user:tom[anothercaveat:{"someparam": 43}]`),
				tuple.MustParse(`resource:oneteam#team@team:first[somecaveat:{"someparam": 42}]`),
			},
			ONR("resource", "oneteam", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAnd(
				caveatAndCtx("anothercaveat", map[string]any{"someparam": int64(43)}),
				caveatAndCtx("somecaveat", map[string]any{"someparam": int64(42)}),
			),
			nil,
		},
		{
			"caveated intersection arrow with multiple caveated branches",
			`definition user {}

  definition team {
    relation direct_member: user
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam >= 42
  }

  definition resource {
    relation team: team with somecaveat
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse(`resource:someresource#team@team:first[somecaveat:{"someparam": 41}]`),
				tuple.MustParse(`resource:someresource#team@team:second[somecaveat:{"someparam": 42}]`),
				tuple.MustParse(`team:first#direct_member@user:tom`),
				tuple.MustParse(`team:second#direct_member@user:tom`),
			},
			ONR("resource", "someresource", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAnd(
				caveatAndCtx("somecaveat", map[string]any{"someparam": int64(41)}),
				caveatAndCtx("somecaveat", map[string]any{"someparam": int64(42)}),
			),
			nil,
		},
		{
			"caveated intersection arrow with multiple caveated members",
			`definition user {}

  definition team {
    relation direct_member: user with somecaveat
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam >= 42
  }

  definition resource {
    relation team: team
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse(`resource:someresource#team@team:first`),
				tuple.MustParse(`resource:someresource#team@team:second`),
				tuple.MustParse(`team:first#direct_member@user:tom[somecaveat:{"someparam": 41}]`),
				tuple.MustParse(`team:second#direct_member@user:tom[somecaveat:{"someparam": 42}]`),
			},
			ONR("resource", "someresource", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAnd(
				caveatAndCtx("somecaveat", map[string]any{"someparam": int64(41)}),
				caveatAndCtx("somecaveat", map[string]any{"someparam": int64(42)}),
			),
			nil,
		},
		{
			"caveated intersection arrow with one caveated branch",
			`definition user {}

  definition team {
    relation direct_member: user
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam >= 42
  }

  definition resource {
    relation team: team with somecaveat | team
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse(`resource:someresource#team@team:first`),
				tuple.MustParse(`resource:someresource#team@team:second[somecaveat:{"someparam": 42}]`),
				tuple.MustParse(`team:first#direct_member@user:tom`),
				tuple.MustParse(`team:second#direct_member@user:tom`),
			},
			ONR("resource", "someresource", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAndCtx("somecaveat", map[string]any{"someparam": int64(42)}),
			nil,
		},
		{
			"caveated intersection arrow with one caveated member",
			`definition user {}

  definition team {
    relation direct_member: user with somecaveat
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam >= 42
  }

  definition resource {
    relation team: team
    permission view_by_all = team.all(member)
  }`,
			[]tuple.Relationship{
				tuple.MustParse(`resource:someresource#team@team:first`),
				tuple.MustParse(`resource:someresource#team@team:second`),
				tuple.MustParse(`team:first#direct_member@user:tom`),
				tuple.MustParse(`team:second#direct_member@user:tom[somecaveat:{"someparam": 42}]`),
			},
			ONR("resource", "someresource", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAndCtx("somecaveat", map[string]any{"someparam": int64(42)}),
			nil,
		},
		{
			"caveated intersection arrow multiple paths to the same subject",
			`definition user {}

  definition team {
    relation direct_member: user
    permission member = direct_member
  }

  caveat somecaveat(someparam int) {
    someparam >= 42
  }

  definition resource {
    relation team: team with somecaveat | team#direct_member with somecaveat
    permission view_by_all = team.all(member) // Note: this points to the same team twice
  }`,
			[]tuple.Relationship{
				tuple.MustParse(`resource:someresource#team@team:first`),
				tuple.MustParse(`resource:someresource#team@team:first#direct_member[somecaveat]`),
				tuple.MustParse(`team:first#direct_member@user:tom`),
			},
			ONR("resource", "someresource", "view_by_all"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatAndCtx("somecaveat", nil),
			nil,
		},
		{
			"recursive all arrow positive result",
			`definition user {}

			definition folder {
				relation parent: folder
				relation owner: user

				permission view = parent.all(owner)
			}

			definition document {
				relation folder: folder
				permission view = folder.all(view)
			}`,
			[]tuple.Relationship{
				tuple.MustParse("folder:root1#owner@user:tom"),
				tuple.MustParse("folder:root1#owner@user:fred"),
				tuple.MustParse("folder:root1#owner@user:sarah"),
				tuple.MustParse("folder:root2#owner@user:fred"),
				tuple.MustParse("folder:root2#owner@user:sarah"),

				tuple.MustParse("folder:child1#parent@folder:root1"),
				tuple.MustParse("folder:child1#parent@folder:root2"),

				tuple.MustParse("folder:child2#parent@folder:root1"),
				tuple.MustParse("folder:child2#parent@folder:root2"),

				tuple.MustParse("document:doc1#folder@folder:child1"),
				tuple.MustParse("document:doc1#folder@folder:child2"),
			},
			ONR("document", "doc1", "view"),
			ONR("user", "fred", "..."),
			v1.ResourceCheckResult_MEMBER,
			nil,
			nil,
		},
		{
			"recursive all arrow negative result",
			`definition user {}

			definition folder {
				relation parent: folder
				relation owner: user

				permission view = parent.all(owner)
			}

			definition document {
				relation folder: folder
				permission view = folder.all(view)
			}`,
			[]tuple.Relationship{
				tuple.MustParse("folder:root1#owner@user:tom"),
				tuple.MustParse("folder:root1#owner@user:fred"),
				tuple.MustParse("folder:root1#owner@user:sarah"),
				tuple.MustParse("folder:root2#owner@user:fred"),
				tuple.MustParse("folder:root2#owner@user:sarah"),

				tuple.MustParse("folder:child1#parent@folder:root1"),
				tuple.MustParse("folder:child1#parent@folder:root2"),

				tuple.MustParse("folder:child2#parent@folder:root1"),
				tuple.MustParse("folder:child2#parent@folder:root2"),

				tuple.MustParse("document:doc1#folder@folder:child1"),
				tuple.MustParse("document:doc1#folder@folder:child2"),
			},
			ONR("document", "doc1", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"recursive all arrow negative result due to recursion missing a folder",
			`definition user {}

			definition folder {
				relation parent: folder
				relation owner: user

				permission view = parent.all(owner)
			}

			definition document {
				relation folder: folder
				permission view = folder.all(view)
			}`,
			[]tuple.Relationship{
				tuple.MustParse("folder:root1#owner@user:tom"),
				tuple.MustParse("folder:root1#owner@user:fred"),
				tuple.MustParse("folder:root1#owner@user:sarah"),
				tuple.MustParse("folder:root2#owner@user:fred"),
				tuple.MustParse("folder:root2#owner@user:sarah"),

				tuple.MustParse("folder:child1#parent@folder:root1"),
				tuple.MustParse("folder:child1#parent@folder:root2"),
				tuple.MustParse("folder:child1#parent@folder:root3"),

				tuple.MustParse("folder:child2#parent@folder:root1"),
				tuple.MustParse("folder:child2#parent@folder:root2"),

				tuple.MustParse("document:doc1#folder@folder:child1"),
				tuple.MustParse("document:doc1#folder@folder:child2"),
			},
			ONR("document", "doc1", "view"),
			ONR("user", "fred", "..."),
			v1.ResourceCheckResult_NOT_MEMBER,
			nil,
			nil,
		},
		{
			"caveated over multiple branches",
			`
			 caveat somecaveat(somevalue int) {
			    somevalue == 42
			 }

  definition user {}

  definition role {
    relation member: user with somecaveat
  }

  definition resource {
      relation viewer: role#member with somecaveat
      permission view = viewer
  }
			`,
			[]tuple.Relationship{
				tuple.MustParse(`role:firstrole#member@user:tom[somecaveat:{"somevalue":40}]`),
				tuple.MustParse(`role:secondrole#member@user:tom[somecaveat:{"somevalue":42}]`),
				tuple.MustParse(`resource:doc1#viewer@role:firstrole#member`),
				tuple.MustParse(`resource:doc1#viewer@role:secondrole#member`),
			},
			ONR("resource", "doc1", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatOr(
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(40)}),
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(42)}),
			),
			caveatOr(
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(42)}),
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(40)}),
			),
		},
		{
			"caveated over multiple branches reversed",
			`
			 caveat somecaveat(somevalue int) {
			    somevalue == 42
			 }

  definition user {}
  
  definition role {
    relation member: user with somecaveat
  }

  definition resource {
      relation viewer: role#member with somecaveat
      permission view = viewer
  }
			`,
			[]tuple.Relationship{
				tuple.MustParse(`role:secondrole#member@user:tom[somecaveat:{"somevalue":42}]`),
				tuple.MustParse(`role:firstrole#member@user:tom[somecaveat:{"somevalue":40}]`),
				tuple.MustParse(`resource:doc1#viewer@role:secondrole#member`),
				tuple.MustParse(`resource:doc1#viewer@role:firstrole#member`),
			},
			ONR("resource", "doc1", "view"),
			ONR("user", "tom", "..."),
			v1.ResourceCheckResult_CAVEATED_MEMBER,
			caveatOr(
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(40)}),
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(42)}),
			),
			caveatOr(
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(42)}),
				caveatAndCtx("somecaveat", map[string]any{"somevalue": int64(40)}),
			),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)

			dispatcher := NewLocalOnlyDispatcher(10, 100)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

			ctx := datastoremw.ContextWithHandle(context.Background())
			require.NoError(datastoremw.SetInContext(ctx, ds))

			resp, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
				ResourceRelation: RR(tc.resource.ObjectType, tc.resource.Relation).ToCoreRR(),
				ResourceIds:      []string{tc.resource.ObjectID},
				Subject:          tc.subject.ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				ResultsSetting: v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
			})
			require.NoError(err)

			membership := v1.ResourceCheckResult_NOT_MEMBER
			if r, ok := resp.ResultsByResourceId[tc.resource.ObjectID]; ok {
				membership = r.Membership
			}

			require.Equal(tc.expectedPermissionship, membership)

			if tc.expectedCaveat != nil && tc.alternativeExpectedCaveat == nil {
				require.NotEmpty(resp.ResultsByResourceId[tc.resource.ObjectID].Expression)
				testutil.RequireProtoEqual(t, tc.expectedCaveat, resp.ResultsByResourceId[tc.resource.ObjectID].Expression, "mismatch in caveat")
			}

			if tc.expectedCaveat != nil && tc.alternativeExpectedCaveat != nil {
				require.NotEmpty(resp.ResultsByResourceId[tc.resource.ObjectID].Expression)

				if testutil.AreProtoEqual(tc.expectedCaveat, resp.ResultsByResourceId[tc.resource.ObjectID].Expression, "mismatch in caveat") != nil {
					testutil.RequireProtoEqual(t, tc.alternativeExpectedCaveat, resp.ResultsByResourceId[tc.resource.ObjectID].Expression, "mismatch in caveat")
				}
			}
		})
	}
}

func addFrame(trace *v1.CheckDebugTrace, foundFrames *mapz.Set[string]) {
	foundFrames.Insert(fmt.Sprintf("%s:%s#%s", trace.Request.ResourceRelation.Namespace, strings.Join(trace.Request.ResourceIds, ","), trace.Request.ResourceRelation.Relation))
	for _, subTrace := range trace.SubProblems {
		addFrame(subTrace, foundFrames)
	}
}

func TestCheckDebugging(t *testing.T) {
	t.Parallel()
	type expectedFrame struct {
		resourceType tuple.RelationReference
		resourceIDs  []string
	}

	testCases := []struct {
		namespace      string
		objectID       string
		permission     string
		subject        tuple.ObjectAndRelation
		expectedFrames []expectedFrame
	}{
		{
			"document", "masterplan", "view",
			ONR("user", "product_manager", graph.Ellipsis),
			[]expectedFrame{
				{
					RR("document", "view"),
					[]string{"masterplan"},
				},
				{
					RR("document", "edit"),
					[]string{"masterplan"},
				},
				{
					RR("document", "owner"),
					[]string{"masterplan"},
				},
			},
		},
		{
			"document", "masterplan", "view_and_edit",
			ONR("user", "product_manager", graph.Ellipsis),
			[]expectedFrame{
				{
					RR("document", "view_and_edit"),
					[]string{"masterplan"},
				},
			},
		},
		{
			"document", "specialplan", "view_and_edit",
			ONR("user", "multiroleguy", graph.Ellipsis),
			[]expectedFrame{
				{
					RR("document", "view_and_edit"),
					[]string{"specialplan"},
				},
				{
					RR("document", "viewer_and_editor"),
					[]string{"specialplan"},
				},
				{
					RR("document", "edit"),
					[]string{"specialplan"},
				},
				{
					RR("document", "editor"),
					[]string{"specialplan"},
				},
			},
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"debugging::%s:%s#%s@%s:%s#%s",
			tc.namespace,
			tc.objectID,
			tc.permission,
			tc.subject.ObjectType,
			tc.subject.ObjectID,
			tc.subject.Relation,
		)

		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)

			ctx, dispatch, revision := newLocalDispatcher(t)

			checkResult, err := dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
				ResourceRelation: RR(tc.namespace, tc.permission).ToCoreRR(),
				ResourceIds:      []string{tc.objectID},
				ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
				Subject:          tc.subject.ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				Debug: v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING,
			})

			require.NoError(err)
			require.NotNil(checkResult.Metadata.DebugInfo)
			require.NotNil(checkResult.Metadata.DebugInfo.Check)
			require.NotNil(checkResult.Metadata.DebugInfo.Check.Duration)

			expectedFrames := mapz.NewSet[string]()
			for _, expectedFrame := range tc.expectedFrames {
				expectedFrames.Add(fmt.Sprintf("%s:%s#%s", expectedFrame.resourceType.ObjectType, strings.Join(expectedFrame.resourceIDs, ","), expectedFrame.resourceType.Relation))
			}

			foundFrames := mapz.NewSet[string]()
			addFrame(checkResult.Metadata.DebugInfo.Check, foundFrames)

			require.Empty(expectedFrames.Subtract(foundFrames).AsSlice(), "missing expected frames: %v", expectedFrames.Subtract(foundFrames).AsSlice())
		})
	}
}

func TestCheckWithHints(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                   string
		schema                 string
		relationships          []tuple.Relationship
		resource               tuple.ObjectAndRelation
		subject                tuple.ObjectAndRelation
		hints                  []*v1.CheckHint
		expectedPermissionship bool
	}{
		{
			"no relationships",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			nil,
			false,
		},
		{
			"no relationships with matching check hint",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			true,
		},
		{
			"no relationships with unrelated check hint",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "anotherdoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			false,
		},
		{
			"no relationships with unrelated check hint due to subject",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "anotheruser", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			false,
		},
		{
			"no relationships with matching arrow check hint",
			`definition user {}
		
   			 definition organization {
				relation member: user
			 }

		 	 definition document {
				relation org: organization
				permission view = org->member
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForArrow("document", "somedoc", "org", "member", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			true,
		},
		{
			"no relationships with non matching tupleset arrow check hint",
			`definition user {}
		
   			 definition organization {
				relation member: user
			 }

		 	 definition document {
				relation org: organization
				permission view = org->member
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForArrow("document", "somedoc", "anotherrel", "member", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			false,
		},
		{
			"no relationships with non matching computed userset arrow check hint",
			`definition user {}
		
   			 definition organization {
				relation member: user
			 }

		 	 definition document {
				relation org: organization
				permission view = org->member
  			 }`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForArrow("document", "somedoc", "org", "membersssss", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			false,
		},
		{
			"check hint over part of an intersection but missing other branch in rels",
			`definition user {}

			definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			false,
		},
		{
			"check hint over part of an intersection with other branch in rels",
			`definition user {}

			definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
			}`,
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#editor@user:tom"),
			},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			true,
		},
		{
			"check hint of wrong type over part of an intersection with other branch in rels",
			`definition user {}

			definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
			}`,
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#editor@user:tom"),
			},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForArrow("document", "somedoc", "viewer", "something", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			false,
		},
		{
			"check hint over part of an exclusion with other branch not in rels",
			`definition user {}

			definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			true,
		},
		{
			"check hint for wildcard",
			`definition user {}

			definition document {
				relation viewer: user:*
				permission view = viewer
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})},
			true,
		},
		{
			"check hint for wildcard, negative",
			`definition user {}

			definition document {
				relation viewer: user:*
				permission view = viewer
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_NOT_MEMBER,
			})},
			false,
		},
		{
			"check hint for each branch of an intersection",
			`definition user {}

			definition document {
				relation viewer: user
				relation editor: user
				permission view = viewer & editor
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{
				hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_MEMBER,
				}),
				hints.CheckHintForComputedUserset("document", "somedoc", "editor", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_MEMBER,
				}),
			},
			true,
		},
		{
			"check hint for each branch of an exclusion",
			`definition user {}

			definition document {
				relation viewer: user
				relation banned: user
				permission view = viewer - banned
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{
				hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_MEMBER,
				}),
				hints.CheckHintForComputedUserset("document", "somedoc", "banned", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_MEMBER,
				}),
			},
			false,
		},
		{
			"check hint for each branch of an exclusion, allowed",
			`definition user {}

			definition document {
				relation viewer: user
				relation banned: user
				permission view = viewer - banned
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{
				hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_MEMBER,
				}),
				hints.CheckHintForComputedUserset("document", "somedoc", "banned", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_NOT_MEMBER,
				}),
			},
			true,
		},
		{
			"check hint for each branch of an exclusion, not member on either branch",
			`definition user {}

			definition document {
				relation viewer: user
				relation banned: user
				permission view = viewer - banned
			}`,
			[]tuple.Relationship{},
			ONR("document", "somedoc", "view"),
			ONR("user", "tom", graph.Ellipsis),
			[]*v1.CheckHint{
				hints.CheckHintForComputedUserset("document", "somedoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_NOT_MEMBER,
				}),
				hints.CheckHintForComputedUserset("document", "somedoc", "banned", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
					Membership: v1.ResourceCheckResult_NOT_MEMBER,
				}),
			},
			false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)

			dispatcher := NewLocalOnlyDispatcher(10, 100)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

			ctx := datastoremw.ContextWithHandle(context.Background())
			require.NoError(datastoremw.SetInContext(ctx, ds))

			resp, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
				ResourceRelation: RR(tc.resource.ObjectType, tc.resource.Relation).ToCoreRR(),
				ResourceIds:      []string{tc.resource.ObjectID},
				Subject:          tc.subject.ToCoreONR(),
				ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				CheckHints: tc.hints,
			})
			require.NoError(err)

			_, ok := resp.ResultsByResourceId[tc.resource.ObjectID]
			if tc.expectedPermissionship {
				require.True(ok)
				require.Equal(v1.ResourceCheckResult_MEMBER, resp.ResultsByResourceId[tc.resource.ObjectID].Membership)
			} else {
				if ok {
					require.Equal(v1.ResourceCheckResult_NOT_MEMBER, resp.ResultsByResourceId[tc.resource.ObjectID].Membership)
				}
			}
		})
	}
}

func TestCheckHintsPartialApplication(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	dispatcher := NewLocalOnlyDispatcher(10, 100)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, `
		definition user {}

		definition document {
			relation viewer: user
			permission view = viewer
		}

	`, []tuple.Relationship{
		tuple.MustParse("document:somedoc#viewer@user:tom"),
	}, require)

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))

	resp, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"somedoc", "anotherdoc", "thirddoc"},
		Subject:          tuple.CoreONR("user", "tom", graph.Ellipsis),
		ResultsSetting:   v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		CheckHints: []*v1.CheckHint{
			hints.CheckHintForComputedUserset("document", "anotherdoc", "viewer", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			}),
		},
	})
	require.NoError(err)

	require.Len(resp.ResultsByResourceId, 2)
	require.Equal(v1.ResourceCheckResult_MEMBER, resp.ResultsByResourceId["somedoc"].Membership)
	require.Equal(v1.ResourceCheckResult_MEMBER, resp.ResultsByResourceId["anotherdoc"].Membership)
}

func TestCheckHintsPartialApplicationOverArrow(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	dispatcher := NewLocalOnlyDispatcher(10, 100)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, `
		definition user {}

		definition organization {
			relation member: user
		}

		definition document {
			relation org: organization
			permission view = org->member
		}

	`, []tuple.Relationship{
		tuple.MustParse("document:somedoc#org@organization:someorg"),
		tuple.MustParse("organization:someorg#member@user:tom"),
	}, require)

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))

	resp, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		ResourceIds:      []string{"somedoc", "anotherdoc", "thirddoc"},
		Subject:          tuple.CoreONR("user", "tom", graph.Ellipsis),
		ResultsSetting:   v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		CheckHints: []*v1.CheckHint{
			hints.CheckHintForArrow("document", "anotherdoc", "org", "member", ONR("user", "tom", graph.Ellipsis), &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			}),
		},
	})
	require.NoError(err)

	require.Len(resp.ResultsByResourceId, 2)
	require.Equal(v1.ResourceCheckResult_MEMBER, resp.ResultsByResourceId["somedoc"].Membership)
	require.Equal(v1.ResourceCheckResult_MEMBER, resp.ResultsByResourceId["anotherdoc"].Membership)
}

func newLocalDispatcherWithConcurrencyLimit(t testing.TB, concurrencyLimit uint16) (context.Context, dispatch.Dispatcher, datastore.Revision) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))

	dispatch := NewLocalOnlyDispatcher(concurrencyLimit, 100)

	cachingDispatcher, err := caching.NewCachingDispatcher(caching.DispatchTestCache(t), false, "", &keys.CanonicalKeyHandler{})
	cachingDispatcher.SetDelegate(dispatch)
	require.NoError(t, err)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	return ctx, cachingDispatcher, revision
}

func newLocalDispatcher(t testing.TB) (context.Context, dispatch.Dispatcher, datastore.Revision) {
	return newLocalDispatcherWithConcurrencyLimit(t, 10)
}

func newLocalDispatcherWithSchemaAndRels(t testing.TB, schema string, rels []tuple.Relationship) (context.Context, dispatch.Dispatcher, datastore.Revision) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, schema, rels, require.New(t))

	dispatch := NewLocalOnlyDispatcher(10, 100)

	cachingDispatcher, err := caching.NewCachingDispatcher(caching.DispatchTestCache(t), false, "", &keys.CanonicalKeyHandler{})
	cachingDispatcher.SetDelegate(dispatch)
	require.NoError(t, err)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	return ctx, cachingDispatcher, revision
}
