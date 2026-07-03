package v1

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/authzed/spicedb/pkg/datalayer"
	mock_datalayer "github.com/authzed/spicedb/pkg/datalayer/mocks"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
)

var (
	testUserDef = namespace.Namespace("user")
	testDocDef  = namespace.Namespace("document",
		namespace.MustRelation("viewer", nil, namespace.AllowedRelation("user", "...")),
		namespace.MustRelation("view",
			namespace.Union(namespace.ComputedUserset("viewer")),
		),
	)
	testTypeDefs = []datastore.RevisionedTypeDefinition{
		{Definition: testUserDef},
		{Definition: testDocDef},
	}
)

func mockReaderExpectSchema(ctrl *gomock.Controller) *mock_datalayer.MockRevisionedReader {
	sr := mock_datalayer.NewMockSchemaReader(ctrl)
	sr.EXPECT().ListAllTypeDefinitions(gomock.Any()).Return(testTypeDefs, nil).AnyTimes()
	sr.EXPECT().ListAllCaveatDefinitions(gomock.Any()).Return([]datastore.RevisionedCaveat{}, nil).AnyTimes()

	reader := mock_datalayer.NewMockRevisionedReader(ctrl)
	reader.EXPECT().ReadSchema(gomock.Any()).Return(sr, nil).AnyTimes()
	return reader
}

func countingReader(ctrl *gomock.Controller, count *atomic.Int32) *mock_datalayer.MockRevisionedReader {
	sr := mock_datalayer.NewMockSchemaReader(ctrl)
	sr.EXPECT().ListAllTypeDefinitions(gomock.Any()).Return(testTypeDefs, nil).AnyTimes()
	sr.EXPECT().ListAllCaveatDefinitions(gomock.Any()).Return([]datastore.RevisionedCaveat{}, nil).AnyTimes()

	reader := mock_datalayer.NewMockRevisionedReader(ctrl)
	reader.EXPECT().ReadSchema(gomock.Any()).DoAndReturn(func(_ any) (datalayer.SchemaReader, error) {
		count.Add(1)
		return sr, nil
	}).AnyTimes()
	return reader
}

func TestGetOrBuildSchema_CachesOnRealHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	hash := datalayer.SchemaHash("abc123")
	ctx := t.Context()

	s1, err := c.getOrBuildSchema(ctx, reader, hash)
	require.NoError(t, err)
	require.NotNil(t, s1)
	require.Equal(t, int32(1), readCount.Load())

	s2, err := c.getOrBuildSchema(ctx, reader, hash)
	require.NoError(t, err)
	require.Equal(t, s1, s2, "second call should return cached schema")
	require.Equal(t, int32(1), readCount.Load(), "ReadSchema should not be called again")
}

func TestGetOrBuildSchema_BypassSentinelSkipsCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	ctx := t.Context()

	_, err := c.getOrBuildSchema(ctx, reader, datalayer.NoSchemaHashForTesting)
	require.NoError(t, err)
	require.Equal(t, int32(1), readCount.Load())

	_, err = c.getOrBuildSchema(ctx, reader, datalayer.NoSchemaHashForTesting)
	require.NoError(t, err)
	require.Equal(t, int32(2), readCount.Load(), "bypass sentinel must not cache")
}

func TestGetOrBuildSchema_DifferentHashesCacheSeparately(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	ctx := t.Context()

	s1, err := c.getOrBuildSchema(ctx, reader, "hash-a")
	require.NoError(t, err)

	s2, err := c.getOrBuildSchema(ctx, reader, "hash-b")
	require.NoError(t, err)
	require.Equal(t, int32(2), readCount.Load())
	require.NotSame(t, s1, s2, "different hashes should produce independent entries")

	// Re-fetch both from cache.
	_, err = c.getOrBuildSchema(ctx, reader, "hash-a")
	require.NoError(t, err)
	_, err = c.getOrBuildSchema(ctx, reader, "hash-b")
	require.NoError(t, err)
	require.Equal(t, int32(2), readCount.Load(), "both should be served from cache")
}

func TestGetOrBuildOutline_CachesOnRealHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	hash := datalayer.SchemaHash("abc123")
	ctx := t.Context()

	co1, err := c.getOrBuildOutline(ctx, reader, hash, "document", "view")
	require.NoError(t, err)
	require.Equal(t, int32(1), readCount.Load())

	co2, err := c.getOrBuildOutline(ctx, reader, hash, "document", "view")
	require.NoError(t, err)
	require.Equal(t, co1, co2, "outline should be returned from cache")
	require.Equal(t, int32(1), readCount.Load())
}

func TestGetOrBuildOutline_BypassSentinelSkipsCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	ctx := t.Context()

	_, err := c.getOrBuildOutline(ctx, reader, datalayer.NoSchemaHashInTransaction, "document", "view")
	require.NoError(t, err)

	_, err = c.getOrBuildOutline(ctx, reader, datalayer.NoSchemaHashInTransaction, "document", "view")
	require.NoError(t, err)
	require.Equal(t, int32(2), readCount.Load(), "bypass sentinel must reload every time")
}

func TestGetOrBuildOutline_DifferentPermissionsCacheSeparately(t *testing.T) {
	ctrl := gomock.NewController(t)
	reader := mockReaderExpectSchema(ctrl)

	c := newQueryPlanCache()
	defer c.Close()

	hash := datalayer.SchemaHash("abc123")
	ctx := t.Context()

	co1, err := c.getOrBuildOutline(ctx, reader, hash, "document", "view")
	require.NoError(t, err)

	co2, err := c.getOrBuildOutline(ctx, reader, hash, "document", "viewer")
	require.NoError(t, err)
	require.NotEqual(t, co1, co2, "different permissions should have different outlines")
}

func TestGetOrBuildSchema_ConcurrentSameHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	hash := datalayer.SchemaHash("concurrent-hash")
	ctx := t.Context()
	const goroutines = 20

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			_, err := c.getOrBuildSchema(ctx, reader, hash)
			// Cannot use require on a separate goroutines. Assert fails the
			// test, but lets all the goroutines run before exit.
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	require.LessOrEqual(t, readCount.Load(), int32(2),
		"singleflight should collapse most concurrent loads into one or two calls")
}

func TestGetOrBuildSchema_ConcurrentBypassSentinel(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	ctx := t.Context()
	const goroutines = 10

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			_, err := c.getOrBuildSchema(ctx, reader, datalayer.NoSchemaHashForTesting)
			// Cannot use require on a separate goroutines. Assert fails the
			// test, but lets all the goroutines run before exit.
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	require.Equal(t, int32(goroutines), readCount.Load(),
		"bypass sentinels must not be deduplicated — each call should load independently")
}

func TestGetOrBuildOutline_SchemaIsCachedAcrossOutlineCalls(t *testing.T) {
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32
	reader := countingReader(ctrl, &readCount)

	c := newQueryPlanCache()
	defer c.Close()

	hash := datalayer.SchemaHash("shared-schema")
	ctx := t.Context()

	_, err := c.getOrBuildOutline(ctx, reader, hash, "document", "view")
	require.NoError(t, err)

	_, err = c.getOrBuildOutline(ctx, reader, hash, "document", "viewer")
	require.NoError(t, err)

	require.Equal(t, int32(1), readCount.Load(),
		"schema should be cached and reused across different outline builds for the same hash")
}

func TestAllBypassSentinelsSkipCache(t *testing.T) {
	sentinels := []datalayer.SchemaHash{
		datalayer.NoSchemaHashInTransaction,
		datalayer.NoSchemaHashInDevelopment,
		datalayer.NoSchemaHashForTesting,
		datalayer.NoSchemaHashForWatch,
		datalayer.NoSchemaHashForLegacyCursor,
		datalayer.NoSchemaHashInLegacyMode,
		datalayer.NoSchemaHashInLegacyZedToken,
	}

	for _, sentinel := range sentinels {
		t.Run(string(sentinel), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var readCount atomic.Int32
			reader := countingReader(ctrl, &readCount)

			c := newQueryPlanCache()
			defer c.Close()

			ctx := t.Context()
			_, err := c.getOrBuildSchema(ctx, reader, sentinel)
			require.NoError(t, err)
			_, err = c.getOrBuildSchema(ctx, reader, sentinel)
			require.NoError(t, err)

			require.Equal(t, int32(2), readCount.Load())
		})
	}
}

func TestGetOrBuildOutline_InvalidPermission(t *testing.T) {
	ctrl := gomock.NewController(t)
	reader := mockReaderExpectSchema(ctrl)

	c := newQueryPlanCache()
	defer c.Close()

	ctx := t.Context()
	_, err := c.getOrBuildOutline(ctx, reader, "hash", "document", "nonexistent")
	require.Error(t, err)
}

func TestGetOrBuildOutline_InvalidResourceType(t *testing.T) {
	ctrl := gomock.NewController(t)
	reader := mockReaderExpectSchema(ctrl)

	c := newQueryPlanCache()
	defer c.Close()

	ctx := t.Context()
	_, err := c.getOrBuildOutline(ctx, reader, "hash", "nonexistent", "view")
	require.Error(t, err)
}

func TestLoadSchema_ReturnsValidSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	reader := mockReaderExpectSchema(ctrl)

	c := newQueryPlanCache()
	defer c.Close()

	s, err := c.loadSchema(t.Context(), reader)
	require.NoError(t, err)
	require.NotNil(t, s)

	docDef, ok := s.GetTypeDefinition("document")
	require.True(t, ok)
	require.NotNil(t, docDef)

	userDef, ok := s.GetTypeDefinition("user")
	require.True(t, ok)
	require.NotNil(t, userDef)
}

func TestGetOrBuildSchema_ErrorOnReadSchema(t *testing.T) {
	ctrl := gomock.NewController(t)

	reader := mock_datalayer.NewMockRevisionedReader(ctrl)
	reader.EXPECT().ReadSchema(gomock.Any()).Return(nil, errForTesting).AnyTimes()

	c := newQueryPlanCache()
	defer c.Close()

	_, err := c.getOrBuildSchema(t.Context(), reader, "hash-err")
	require.ErrorIs(t, err, errForTesting)
}

func TestGetOrBuildSchema_ErrorOnListTypeDefinitions(t *testing.T) {
	ctrl := gomock.NewController(t)

	sr := mock_datalayer.NewMockSchemaReader(ctrl)
	sr.EXPECT().ListAllTypeDefinitions(gomock.Any()).Return(nil, errForTesting)

	reader := mock_datalayer.NewMockRevisionedReader(ctrl)
	reader.EXPECT().ReadSchema(gomock.Any()).Return(sr, nil)

	c := newQueryPlanCache()
	defer c.Close()

	_, err := c.getOrBuildSchema(t.Context(), reader, "hash-err")
	require.ErrorIs(t, err, errForTesting)
}

func TestGetOrBuildOutline_ErrorNotCached(t *testing.T) {
	ctrl := gomock.NewController(t)

	// First call: reader returns an error.
	sr := mock_datalayer.NewMockSchemaReader(ctrl)
	sr.EXPECT().ListAllTypeDefinitions(gomock.Any()).Return(nil, errForTesting)

	errReader := mock_datalayer.NewMockRevisionedReader(ctrl)
	errReader.EXPECT().ReadSchema(gomock.Any()).Return(sr, nil)

	c := newQueryPlanCache()
	defer c.Close()

	_, err := c.getOrBuildOutline(t.Context(), errReader, "hash-err", "document", "view")
	require.Error(t, err)
}

func TestGetOrBuildOutline_UsesSchemaCache(t *testing.T) {
	// Verifies that two outline lookups for different permissions on the same
	// schema hash only call ReadSchema once (the schema layer caches it).
	ctrl := gomock.NewController(t)
	var readCount atomic.Int32

	docDef := namespace.Namespace("document",
		namespace.MustRelation("viewer", nil, namespace.AllowedRelation("user", "...")),
		namespace.MustRelation("editor", nil, namespace.AllowedRelation("user", "...")),
		namespace.MustRelation("view",
			namespace.Union(namespace.ComputedUserset("viewer")),
		),
		namespace.MustRelation("edit",
			namespace.Union(namespace.ComputedUserset("editor")),
		),
	)

	tdefs := []datastore.RevisionedTypeDefinition{
		{Definition: namespace.Namespace("user")},
		{Definition: docDef},
	}

	sr := mock_datalayer.NewMockSchemaReader(ctrl)
	sr.EXPECT().ListAllTypeDefinitions(gomock.Any()).Return(tdefs, nil).AnyTimes()
	sr.EXPECT().ListAllCaveatDefinitions(gomock.Any()).Return([]datastore.RevisionedCaveat{}, nil).AnyTimes()

	// Override the reader to use our custom type defs while still counting.
	customReader := mock_datalayer.NewMockRevisionedReader(ctrl)
	customReader.EXPECT().ReadSchema(gomock.Any()).DoAndReturn(func(_ any) (datalayer.SchemaReader, error) {
		readCount.Add(1)
		return sr, nil
	}).AnyTimes()

	c := newQueryPlanCache()
	defer c.Close()

	hash := datalayer.SchemaHash("multi-perm")
	ctx := t.Context()

	_, err := c.getOrBuildOutline(ctx, customReader, hash, "document", "view")
	require.NoError(t, err)

	_, err = c.getOrBuildOutline(ctx, customReader, hash, "document", "edit")
	require.NoError(t, err)

	require.Equal(t, int32(1), readCount.Load())
}

var errForTesting = &testError{}

type testError struct{}

func (e *testError) Error() string { return "test error" }
