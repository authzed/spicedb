package membership

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

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
			fs("user", "*", "...", ONR("user", "user1", "...")),
			"user:* - {user:user1}",
		},
		{
			"with some exclusion",
			fs("user", "*", "...",
				ONR("user", "user1", "..."),
				ONR("user", "user2", "..."),
				ONR("user", "user3", "..."),
				ONR("user", "user4", "..."),
				ONR("user", "user5", "..."),
			),
			"user:* - {user:user1, user:user2, user:user3, user:user4, user:user5}",
		},
		{
			"with many exclusion",
			fs("user", "*", "...",
				ONR("user", "user1", "..."),
				ONR("user", "user2", "..."),
				ONR("user", "user3", "..."),
				ONR("user", "user4", "..."),
				ONR("user", "user5", "..."),
				ONR("user", "user6", "..."),
			),
			"user:* - {user:user1, user:user2, user:user3, user:user4, user:user5, user:user6}",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			require.Equal(tc.expected, tc.fs.ToValidationString())

			sub, err := blocks.ValidationString(fmt.Sprintf("[%s]", tc.expected)).Subject()
			require.Nil(err)
			require.NotNil(sub)
		})
	}
}
