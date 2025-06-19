package development

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func TestRunAllAssertionsWithCaveatedFailures(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
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

	require.NoError(t, err)
	require.Nil(t, devErrs)

	assertions := &blocks.Assertions{
		AssertCaveated: []blocks.Assertion{
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

func TestRunAllAssertionsWithFalseAssertions(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
		Relationships: []*core.RelationTuple{},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	assertions := &blocks.Assertions{
		AssertFalse: []blocks.Assertion{
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

func TestRunAllAssertionsWithCaveatOnAssertion(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

caveat somecaveat(somecondition int) {
	somecondition == 42
}

definition document {
	relation viewer: user with somecaveat
}
`,
		Relationships: []*core.RelationTuple{},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	// Create an assertion with a caveat, which should result in an error
	rel := tuple.MustParse("document:somedoc#viewer@user:someuser[somecaveat]")
	relString := tuple.MustString(rel)
	assertions := &blocks.Assertions{
		AssertTrue: []blocks.Assertion{
			{
				RelationshipWithContextString: relString,
				Relationship:                  rel,
			},
		},
	}

	adErrs, err := RunAllAssertions(devCtx, assertions)
	require.NoError(t, err)
	require.Len(t, adErrs, 1)
	require.Equal(t, devinterface.DeveloperError_UNKNOWN_RELATION, adErrs[0].Kind)
	require.Contains(t, adErrs[0].Message, "cannot specify a caveat on an assertion")
}

func TestRunAllAssertionsFailure(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
		Relationships: []*core.RelationTuple{},
	})

	require.NoError(t, err)
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
	require.Len(t, adErrs, 1)
	require.Equal(t, devinterface.DeveloperError_ASSERTION_FAILED, adErrs[0].Kind)
	require.Contains(t, adErrs[0].Message, "Expected relation or permission")
}
