package developmentmembership

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func cfs(subjectType string, subjectID string, subjectRel string, excludedSubjectIDs []string, caveatName string) FoundSubject {
	excludedSubjects := make([]FoundSubject, 0, len(excludedSubjectIDs))
	for _, excludedSubjectID := range excludedSubjectIDs {
		excludedSubjects = append(excludedSubjects, FoundSubject{subject: tuple.ONR(subjectType, excludedSubjectID, subjectRel)})
	}

	return FoundSubject{
		subject:          tuple.ONR(subjectType, subjectID, subjectRel),
		excludedSubjects: excludedSubjects,
		resources:        NewONRSet(),
		caveatExpression: caveats.CaveatExprForTesting(caveatName),
	}
}

func TestToValidationString(t *testing.T) {
	testCases := []struct {
		name     string
		fs       FoundSubject
		expected string
	}{
		{
			"basic",
			fs("user", "user1", "..."),
			"user:user1",
		},
		{
			"with exclusion",
			fs("user", "*", "...", "user1"),
			"user:* - {user:user1}",
		},
		{
			"with some exclusion",
			fs("user", "*", "...",
				"user1", "user2", "user3", "user4", "user5",
			),
			"user:* - {user:user1, user:user2, user:user3, user:user4, user:user5}",
		},
		{
			"with many exclusion",
			fs("user", "*", "...",
				"user1", "user2", "user3", "user4", "user5", "user6",
			),
			"user:* - {user:user1, user:user2, user:user3, user:user4, user:user5, user:user6}",
		},
		{
			"caveated",
			cfs("user", "tom", "...", nil, "somecaveat"),
			"user:tom[...]",
		},
		{
			"caveated wildcard",
			cfs("user", "*", "...", []string{"foo", "bar"}, "somecaveat"),
			"user:*[...] - {user:bar, user:foo}",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			require.Equal(tc.expected, tc.fs.ToValidationString())

			sub, err := blocks.ValidationString(fmt.Sprintf("[%s]", tc.expected)).Subject()
			require.Nil(err)
			require.NotNil(sub)
		})
	}
}
