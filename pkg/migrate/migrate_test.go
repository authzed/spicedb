package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeDriver struct{}

func (*fakeDriver) Version(ctx context.Context) (string, error) {
	return "", ctx.Err()
}

func (*fakeDriver) Transact(ctx context.Context, f MigrationFunc[fakeTransaction], new, old string) error {
	return f(ctx, fakeTransaction{})
}

func (*fakeDriver) Close(ctx context.Context) error {
	return ctx.Err()
}

type fakeTransaction struct{}

func TestContextError(t *testing.T) {
	req := require.New(t)
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(1*time.Millisecond))
	m := NewManager[Driver[fakeTransaction], fakeTransaction]()

	err := m.Register("1", "", func(ctx context.Context, tx fakeTransaction) error {
		cancelFunc()
		return nil
	})
	req.NoError(err)

	err = m.Register("2", "1", func(ctx context.Context, fx fakeTransaction) error {
		panic("the second migration should never be executed")
	})
	req.NoError(err)

	err = m.Run(ctx, &fakeDriver{}, Head, false)
	req.ErrorIs(err, context.Canceled)
}

type revisionRangeTest struct {
	start, end        string
	expectError       bool
	expectedRevisions []string
}

func TestRevisionWalking(t *testing.T) {
	testCases := []struct {
		migrations map[string]migration[fakeTransaction]
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
		migrations   map[string]migration[fakeTransaction]
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
		m := Manager[Driver[fakeTransaction], fakeTransaction]{migrations: tc.migrations}
		head, err := m.HeadRevision()
		require.Equal(tc.expectError, err != nil, err)
		require.Equal(tc.headRevision, head)
	}
}

func TestIsHeadCompatible(t *testing.T) {
	testCases := []struct {
		migrations       map[string]migration[fakeTransaction]
		currentMigration string
		expectedResult   bool
		expectError      bool
	}{
		{noMigrations, "", false, true},
		{simpleMigrations, "123", true, false},
		{singleHeadedChain, "789", true, false},
		{singleHeadedChain, "456", true, false},
		{singleHeadedChain, "123", false, false},
		{multiHeadedChain, "", false, true},
		{missingEarlyMigrations, "10", true, false},
		{missingEarlyMigrations, "789", true, false},
		{missingEarlyMigrations, "456", false, false},
	}

	req := require.New(t)
	for _, tc := range testCases {
		m := Manager[Driver[fakeTransaction], fakeTransaction]{migrations: tc.migrations}
		compatible, err := m.IsHeadCompatible(tc.currentMigration)
		req.Equal(compatible, tc.expectedResult)
		req.Equal(tc.expectError, err != nil, err)
	}
}

var noMigrations = map[string]migration[fakeTransaction]{}

var simpleMigrations = map[string]migration[fakeTransaction]{
	"123": {"123", "", nil},
}

var singleHeadedChain = map[string]migration[fakeTransaction]{
	"123": {"123", "", nil},
	"456": {"456", "123", nil},
	"789": {"789", "456", nil},
}

var multiHeadedChain = map[string]migration[fakeTransaction]{
	"123":  {"123", "", nil},
	"456":  {"456", "123", nil},
	"789a": {"789a", "456", nil},
	"789b": {"789b", "456", nil},
}

var missingEarlyMigrations = map[string]migration[fakeTransaction]{
	"456": {"456", "123", nil},
	"789": {"789", "456", nil},
	"10":  {"10", "789", nil},
}
