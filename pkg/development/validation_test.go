package development

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/developmentmembership"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func TestRunValidationSuccess(t *testing.T) {
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

	validation, devErr := ParseExpectedRelationsYAML(`document:somedoc#view:
  - '[user:someuser] is <document:somedoc#viewer>'`)
	require.Nil(t, devErr)
	require.NotNil(t, validation)

	ms, devErrors, err := RunValidation(devCtx, validation)
	require.NoError(t, err)
	require.Nil(t, devErrors)
	require.NotNil(t, ms)
}

func TestRunValidationMissingSubject(t *testing.T) {
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

	validation, devErr := ParseExpectedRelationsYAML(`document:somedoc#view:
  - '[user:someuser] is <document:somedoc#viewer>'`)
	require.Nil(t, devErr)
	require.NotNil(t, validation)

	ms, devErrors, err := RunValidation(devCtx, validation)
	require.NoError(t, err)
	require.Len(t, devErrors, 1)
	require.Equal(t, devinterface.DeveloperError_MISSING_EXPECTED_RELATIONSHIP, devErrors[0].Kind)
	require.Contains(t, devErrors[0].Message, "missing expected subject")
	require.NotNil(t, ms)
}

func TestRunValidationExtraSubject(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:someuser").ToCoreTuple(),
			tuple.MustParse("document:somedoc#viewer@user:anotheruser").ToCoreTuple(),
		},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	validation, devErr := ParseExpectedRelationsYAML(`document:somedoc#view:
  - '[user:someuser] is <document:somedoc#viewer>'`)
	require.Nil(t, devErr)
	require.NotNil(t, validation)

	ms, devErrors, err := RunValidation(devCtx, validation)
	require.NoError(t, err)
	require.NotEmpty(t, devErrors) // Should have some errors due to extra/missing subjects
	require.NotNil(t, ms)
}

func TestRunValidationWithWildcard(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user | user:*
	permission view = viewer
}
`,
		Relationships: []*core.RelationTuple{
			tuple.MustParse("document:somedoc#viewer@user:*").ToCoreTuple(),
		},
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	validation, devErr := ParseExpectedRelationsYAML(`document:somedoc#view:
  - '[user:*] is <document:somedoc#viewer>'`)
	require.Nil(t, devErr)
	require.NotNil(t, validation)

	ms, devErrors, err := RunValidation(devCtx, validation)
	require.NoError(t, err)
	require.Nil(t, devErrors) // Should pass now
	require.NotNil(t, ms)
}

func TestRunValidationInvalidONR(t *testing.T) {
	devCtx, devErrs, err := NewDevContext(t.Context(), &devinterface.RequestContext{
		Schema: `definition user {}

definition document {
	relation viewer: user
}
`,
	})

	require.NoError(t, err)
	require.Nil(t, devErrs)

	validation, devErr := ParseExpectedRelationsYAML(`document:somedoc#nonexistent:
  - '[user:someuser] is <document:somedoc#viewer>'`)
	require.Nil(t, devErr)
	require.NotNil(t, validation)

	ms, devErrors, err := RunValidation(devCtx, validation)
	require.NoError(t, err)
	require.NotEmpty(t, devErrors) // Should have errors due to invalid relation
	require.NotNil(t, ms)
}

func TestGenerateValidation(t *testing.T) {
	membershipSet := developmentmembership.NewMembershipSet()

	// Create a mock found subject
	onr, err := tuple.ParseONR("document:doc1#view")
	require.NoError(t, err)
	_ = onr

	// Add some test data to the membership set
	// Note: This is a simplified test since we can't easily mock the complex membership set structure

	generated, err := GenerateValidation(membershipSet)
	require.NoError(t, err)
	require.Equal(t, "{}\n", generated) // Empty membership set should generate empty YAML
}

func TestWrapResources(t *testing.T) {
	resources := []string{
		"document:doc1#view",
		"document:doc2#edit",
		"user:user1",
	}

	wrapped := wrapResources(resources)
	require.Len(t, wrapped, 3)
	require.Contains(t, wrapped, "<document:doc1#view>")
	require.Contains(t, wrapped, "<document:doc2#edit>")
	require.Contains(t, wrapped, "<user:user1>")

	// Check that they're sorted
	for i := 1; i < len(wrapped); i++ {
		require.LessOrEqual(t, wrapped[i-1], wrapped[i])
	}
}

func TestToExpectedRelationshipsStrings(t *testing.T) {
	subs := []blocks.SubjectAndCaveat{
		{
			Subject:    tuple.MustParseSubjectONR("user:user1"),
			IsCaveated: false,
		},
		{
			Subject:    tuple.MustParseSubjectONR("user:user2"),
			IsCaveated: true,
		},
	}

	strs := toExpectedRelationshipsStrings(subs)
	require.Len(t, strs, 2)
	require.Contains(t, strs, "user:user1")
	require.Contains(t, strs, "user:user2[...]")
}

func TestToFoundRelationshipsStrings(t *testing.T) {
	// This is harder to test without creating actual FoundSubject instances
	// but we can test the empty case
	subs := []developmentmembership.FoundSubject{}
	strs := toFoundRelationshipsStrings(subs)
	require.Empty(t, strs)
}

func TestRunValidationWithNilExpectedSubject(t *testing.T) {
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

	// Create a validation with a nil expected subject to test error handling
	validation, devErr := ParseExpectedRelationsYAML(`document:somedoc#view: []`)
	require.Nil(t, devErr)
	require.NotNil(t, validation)

	ms, devErrors, err := RunValidation(devCtx, validation)
	require.NoError(t, err)
	require.Empty(t, devErrors) // No errors since no subjects are expected or found
	require.NotNil(t, ms)
}
