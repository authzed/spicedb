package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	noNonatomicMigration MigrationFunc[fakeConnPool]
	noTxMigration        TxMigrationFunc[fakeTx]
)

type fakeDriver struct {
	currentVersion string
}

func (fd *fakeDriver) Version(ctx context.Context) (string, error) {
	return fd.currentVersion, ctx.Err()
}

func (fd *fakeDriver) WriteVersion(ctx context.Context, _ fakeTx, to, _ string) error {
	if ctx.Err() == nil {
		fd.currentVersion = to
	}
	return ctx.Err()
}

func (*fakeDriver) Conn() fakeConnPool {
	return fakeConnPool{}
}

func (*fakeDriver) RunTx(ctx context.Context, _ TxMigrationFunc[fakeTx]) error {
	return ctx.Err()
}

func (*fakeDriver) Close(ctx context.Context) error {
	return ctx.Err()
}

type fakeConnPool struct{}

type fakeTx struct{}

func TestContextError(t *testing.T) {
	req := require.New(t)
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(1*time.Millisecond))
	m := NewManager[Driver[fakeConnPool, fakeTx], fakeConnPool, fakeTx]()

	err := m.Register("1", "", func(ctx context.Context, conn fakeConnPool) error {
		cancelFunc()
		return nil
	}, noTxMigration)
	req.NoError(err)

	err = m.Register("2", "1", func(ctx context.Context, conn fakeConnPool) error {
		panic("the second migration should never be executed")
	}, noTxMigration)
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
		migrations map[string]migration[fakeConnPool, fakeTx]
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
		migrations   map[string]migration[fakeConnPool, fakeTx]
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
		m := Manager[Driver[fakeConnPool, fakeTx], fakeConnPool, fakeTx]{migrations: tc.migrations}
		head, err := m.HeadRevision()
		require.Equal(tc.expectError, err != nil, err)
		require.Equal(tc.headRevision, head)
	}
}

func TestIsHeadCompatible(t *testing.T) {
	testCases := []struct {
		migrations       map[string]migration[fakeConnPool, fakeTx]
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
		m := Manager[Driver[fakeConnPool, fakeTx], fakeConnPool, fakeTx]{migrations: tc.migrations}
		compatible, err := m.IsHeadCompatible(tc.currentMigration)
		req.Equal(compatible, tc.expectedResult)
		req.Equal(tc.expectError, err != nil, err)
	}
}

func TestManagerEnsureVersionIsWritten(t *testing.T) {
	req := require.New(t)
	m := NewManager[Driver[fakeConnPool, fakeTx], fakeConnPool, fakeTx]()
	err := m.Register("0", "", noNonatomicMigration, noTxMigration)
	req.NoError(err)
	drv := &fakeDriver{}
	err = m.Run(context.Background(), drv, "0", LiveRun)
	req.Error(err)

	writtenVer, err := drv.Version(context.Background())
	req.NoError(err)
	req.Equal("", writtenVer)
}

var noMigrations = map[string]migration[fakeConnPool, fakeTx]{}

var simpleMigrations = map[string]migration[fakeConnPool, fakeTx]{
	"123": {"123", "", noNonatomicMigration, noTxMigration},
}

var singleHeadedChain = map[string]migration[fakeConnPool, fakeTx]{
	"123": {"123", "", noNonatomicMigration, noTxMigration},
	"456": {"456", "123", noNonatomicMigration, noTxMigration},
	"789": {"789", "456", noNonatomicMigration, noTxMigration},
}

var multiHeadedChain = map[string]migration[fakeConnPool, fakeTx]{
	"123":  {"123", "", noNonatomicMigration, noTxMigration},
	"456":  {"456", "123", noNonatomicMigration, noTxMigration},
	"789a": {"789a", "456", noNonatomicMigration, noTxMigration},
	"789b": {"789b", "456", noNonatomicMigration, noTxMigration},
}

var missingEarlyMigrations = map[string]migration[fakeConnPool, fakeTx]{
	"456": {"456", "123", noNonatomicMigration, noTxMigration},
	"789": {"789", "456", noNonatomicMigration, noTxMigration},
	"10":  {"10", "789", noNonatomicMigration, noTxMigration},
}
