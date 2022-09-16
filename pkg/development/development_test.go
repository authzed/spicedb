package development

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func TestDevelopment(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(context.Background(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser"),
		},
	})

	require.Nil(t, err)
	require.Nil(t, devErrs)

	assertions := &blocks.Assertions{
		AssertTrue: []blocks.Assertion{
			{
				RelationshipString: "document:somedoc#viewer@user:someuser",
				Relationship:       tuple.MustToRelationship(tuple.MustParse("document:somedoc#viewer@user:someuser")),
			},
		},
	}

	adErrs, err := RunAllAssertions(devCtx, assertions)
	require.NoError(t, err)
	require.Nil(t, adErrs)
}

func TestDevelopmentInvalidRelationship(t *testing.T) {
	_, _, err := NewDevContext(context.Background(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
		Relationships: []*core.RelationTuple{
			{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: "document",
					ObjectId:  "*",
					Relation:  "view",
				},
				Subject: &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "tom",
					Relation:  "...",
				},
			},
		},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid resource id")
}
