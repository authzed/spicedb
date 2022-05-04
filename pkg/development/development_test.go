package development

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func TestDevelopment(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(context.Background(), &v0.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
		Relationships: []*v0.RelationTuple{
			core.ToV0RelationTuple(tuple.MustParse("document:somedoc#viewer@user:someuser")),
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
