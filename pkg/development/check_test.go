package development

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestRunCheck(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser").ToCoreTuple(),
		},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	// Test a successful check
	result, err := RunCheck(devCtx,
		tuple.MustParseONR("document:somedoc#view"),
		tuple.MustParseSubjectONR("user:someuser"),
		map[string]any{})

	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_MEMBER, result.Permissionship)
	require.Empty(t, result.MissingCaveatFields)
	require.NotNil(t, result.DispatchDebugInfo)
	require.NotNil(t, result.V1DebugInfo)
}

func TestRunCheckNotMember(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}
`,
		Relationships: []*core.RelationTuple{},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	// Test a check that should return NOT_MEMBER
	result, err := RunCheck(devCtx,
		tuple.MustParseONR("document:somedoc#view"),
		tuple.MustParseSubjectONR("user:someuser"),
		map[string]any{})

	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_NOT_MEMBER, result.Permissionship)
}

func TestRunCheckWithCaveat(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

caveat somecaveat(somecondition int) {
	somecondition == 42
}

definition document {
	relation viewer: user with somecaveat
	permission view = viewer
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser[somecaveat]").ToCoreTuple(),
		},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	// Test a check with a caveat context
	result, err := RunCheck(devCtx,
		tuple.MustParseONR("document:somedoc#view"),
		tuple.MustParseSubjectONR("user:someuser"),
		map[string]any{"somecondition": int64(42)})

	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_MEMBER, result.Permissionship)
}

func TestRunCheckCaveatedMember(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

caveat somecaveat(somecondition int) {
	somecondition == 42
}

definition document {
	relation viewer: user with somecaveat
	permission view = viewer
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser[somecaveat]").ToCoreTuple(),
		},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	// Test a check without caveat context should return CAVEATED_MEMBER
	result, err := RunCheck(devCtx,
		tuple.MustParseONR("document:somedoc#view"),
		tuple.MustParseSubjectONR("user:someuser"),
		map[string]any{})

	require.NoError(t, err)
	require.Equal(t, v1.ResourceCheckResult_CAVEATED_MEMBER, result.Permissionship)
	require.Contains(t, result.MissingCaveatFields, "somecondition")
}
