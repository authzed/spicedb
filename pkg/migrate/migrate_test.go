package migrate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeDriver struct {
}

func (*fakeDriver) Version() (string, error) {
	return "", nil
}

func (*fakeDriver) WriteVersion(version, replaced string) error {
	return nil
}

func TestParameterChecking(t *testing.T) {
	testCases := []struct {
		driver      Driver
		up          interface{}
		expectError bool
	}{
		{&fakeDriver{}, func(d Driver) {}, false},
		{&fakeDriver{}, func(fd *fakeDriver) {}, false},
		{nil, nil, true},
		{&fakeDriver{}, nil, true},
		{&fakeDriver{}, func(a *int) {}, true},
		{nil, func(a *int) {}, true},
		{nil, &fakeDriver{}, true},
	}

	require := require.New(t)
	for _, tc := range testCases {
		err := checkTypes(tc.driver, tc.up)
		require.Equal(tc.expectError, err != nil, err)
	}
}

type revisionRangeTest struct {
	start, end        string
	expectError       bool
	expectedRevisions []string
}

func TestRevisionWalking(t *testing.T) {
	testCases := []struct {
		migrations map[string]migration
		ranges     []revisionRangeTest
	}{
		{noMigrations, []revisionRangeTest{
			{"", "", false, []string{}},
			{"", "123", true, []string{}},
		}},
		{simpleMigrations, []revisionRangeTest{
			{"", "123", false, []string{"123"}},
			{"123", "123", false, []string{}},
			{"", "", false, []string{}},
			{"", "456", true, []string{}},
			{"123", "456", true, []string{}},
			{"456", "456", false, []string{}},
		}},
		{singleHeadedChain, []revisionRangeTest{
			{"", "123", false, []string{"123"}},
			{"", "789", false, []string{"123", "456", "789"}},
			{"123", "789", false, []string{"456", "789"}},
			{"123", "456", false, []string{"456"}},
			{"123", "10", true, []string{}},
			{"", "10", true, []string{}},
		}},
		{multiHeadedChain, []revisionRangeTest{
			{"", "123", false, []string{"123"}},
			{"", "789a", false, []string{"123", "456", "789a"}},
			{"", "789b", false, []string{"123", "456", "789b"}},
			{"456", "789b", false, []string{"789b"}},
		}},
		{missingEarlyMigrations, []revisionRangeTest{
			{"", "123", true, []string{}},
			{"", "10", true, []string{}},
			{"123", "10", false, []string{"456", "789", "10"}},
			{"456", "10", false, []string{"789", "10"}},
		}},
	}

	require := require.New(t)

	for _, tc := range testCases {
		for _, versionRange := range tc.ranges {
			computed, err := collectMigrationsInRange(
				versionRange.start,
				versionRange.end,
				tc.migrations,
			)

			require.Equal(versionRange.expectError, err != nil, err)

			migrationNames := make([]string, 0, len(computed))
			for _, mgr := range computed {
				migrationNames = append(migrationNames, mgr.version)
			}
			require.Equal(versionRange.expectedRevisions, migrationNames)
		}
	}
}

func TestComputeHeadRevision(t *testing.T) {
	testCases := []struct {
		migrations   map[string]migration
		headRevision string
		expectError  bool
	}{
		{noMigrations, "", true},
		{simpleMigrations, "123", false},
		{singleHeadedChain, "789", false},
		{multiHeadedChain, "", true},
		{missingEarlyMigrations, "10", false},
	}

	require := require.New(t)
	for _, tc := range testCases {
		m := Manager{migrations: tc.migrations}
		head, err := m.HeadRevision()
		require.Equal(tc.expectError, err != nil, err)
		require.Equal(tc.headRevision, head)
	}
}

var noMigrations = map[string]migration{}

var simpleMigrations = map[string]migration{
	"123": {"123", "", nil},
}

var singleHeadedChain = map[string]migration{
	"123": {"123", "", nil},
	"456": {"456", "123", nil},
	"789": {"789", "456", nil},
}

var multiHeadedChain = map[string]migration{
	"123":  {"123", "", nil},
	"456":  {"456", "123", nil},
	"789a": {"789a", "456", nil},
	"789b": {"789b", "456", nil},
}

var missingEarlyMigrations = map[string]migration{
	"456": {"456", "123", nil},
	"789": {"789", "456", nil},
	"10":  {"10", "789", nil},
}
