package development

import (
	"context"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func TestDevelopment(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	devCtx, devErrs, err := NewDevContext(context.Background(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser").ToCoreTuple(),
		},
	})

	require.Nil(t, err)
	require.Nil(t, devErrs)

	assertions := &blocks.Assertions{
		AssertTrue: []blocks.Assertion{
			{
				RelationshipWithContextString: "document:somedoc#viewer@user:someuser",
				Relationship:                  tuple.MustParse("document:somedoc#viewer@user:someuser"),
			},
		},
	}

	adErrs, err := RunAllAssertions(devCtx, assertions)
	require.NoError(t, err)
	require.Nil(t, adErrs)
}

func TestDevelopmentInvalidRelationship(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

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
	require.ErrorContains(t, err, "invalid resource id; must match")
}

func TestDevelopmentCaveatedExpectedRels(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	devCtx, devErrs, err := NewDevContext(context.Background(), &devinterface.RequestContext{
		Schema: `definition user {}

caveat somecaveat(somecondition int) {
	somecondition == 42
}

definition document {
	relation viewer: user with somecaveat
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser[somecaveat]").ToCoreTuple(),
		},
	})

	require.Nil(t, err)
	require.Nil(t, devErrs)

	validation, devErr := ParseExpectedRelationsYAML(`document:somedoc#viewer: []`)
	require.Nil(t, devErr)
	require.NotNil(t, validation)

	ms, _, err := RunValidation(devCtx, validation)
	require.Nil(t, err)

	generated, err := GenerateValidation(ms)
	require.Nil(t, err)

	require.Equal(t, "document:somedoc#viewer:\n- '[user:someuser[...]] is <document:somedoc#viewer>'\n", generated)
}

func TestDevContextV1Service(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"), goleak.IgnoreCurrent())

	devCtx, devErrs, err := NewDevContext(context.Background(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser").ToCoreTuple(),
		},
	})

	require.Nil(t, err)
	require.Nil(t, devErrs)

	conn, shutdown, err := devCtx.RunV1InMemoryService()
	require.Nil(t, err)
	t.Cleanup(shutdown)

	client := v1.NewSchemaServiceClient(conn)
	resp, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.Nil(t, err)
	require.Contains(t, resp.SchemaText, "definition document")

	shutdown()
}
