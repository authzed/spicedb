package graph

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy/poolsim"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// The tests in this file run each dispatch operation against a datastore fronted by a
// simulated connection pool (see poolsim), to detect code that requests a second pooled
// connection while already holding one. With a real SQL datastore, such code deadlocks
// the read pool once every connection is held by a caller waiting for another
// (https://github.com/authzed/spicedb/issues/3433).

const poolNestingSchema = `
	caveat allowed(flag bool) {
		flag
	}

	definition user {}

	definition group {
		relation member: user | user with allowed | group#member
	}

	definition document {
		relation viewer: user | user with allowed | group#member
		relation banned: user
		relation parent: folder
		relation member_group: group

		permission view = viewer
		permission view_unless_banned = viewer - banned
		permission view_via_parent = parent->view
		permission view_via_all_groups = member_group.all(member)
	}

	definition folder {
		relation viewer: user | user with allowed | group#member
		permission view = viewer
	}
`

var poolNestingRelationships = []tuple.Relationship{
	// Caveated direct relationship, as in the issue.
	tuple.MustParse(`document:caveated#viewer@user:alice[allowed:{"flag":true}]`),

	// Nested group membership, requiring a recursive dispatch.
	tuple.MustParse("group:inner#member@user:bob"),
	tuple.MustParse("group:outer#member@group:inner#member"),
	tuple.MustParse("document:nested#viewer@group:outer#member"),

	// Caveated nested group membership.
	tuple.MustParse(`group:caveatedgroup#member@user:carol[allowed:{"flag":true}]`),
	tuple.MustParse("document:caveatednested#viewer@group:caveatedgroup#member"),

	// Exclusion, which LookupResources filters with a check.
	tuple.MustParse("document:excluded#viewer@user:dan"),
	tuple.MustParse("document:excluded#banned@user:dan"),
	tuple.MustParse("document:notexcluded#viewer@user:dan"),

	// Arrow.
	tuple.MustParse("folder:somefolder#viewer@group:outer#member"),
	tuple.MustParse("document:infolder#parent@folder:somefolder"),

	// Intersection arrow.
	tuple.MustParse("document:allgroups#member_group@group:inner"),
}

// fanoutGroupCount is larger than both the dispatch chunk size used for testing (100) and the
// LookupResources datastore page size (10 chunks), so that LookupResources hands full chunks to
// its mapper, and reads more than one page of relationships.
const fanoutGroupCount = 1050

func init() {
	for i := range fanoutGroupCount {
		poolNestingRelationships = append(poolNestingRelationships,
			tuple.MustParse(fmt.Sprintf("group:fanout%d#member@user:erin", i)),
		)
	}
	poolNestingRelationships = append(poolNestingRelationships,
		tuple.MustParse("group:fanout0#member@user:frank"),
		tuple.MustParse("document:fanout#viewer@group:fanout0#member"),
	)
	for i := 1; i < fanoutGroupCount; i++ {
		poolNestingRelationships = append(poolNestingRelationships,
			tuple.MustParse(fmt.Sprintf("document:fanout#viewer@group:fanout%d#member", i)),
		)
	}
}

type poolNestingCase struct {
	name       string
	permission string
	subject    tuple.ObjectAndRelation
	expected   []string
}

var poolNestingLookupCases = []poolNestingCase{
	{"caveated direct relationship (#3433)", "view", tuple.ONR("user", "alice", "..."), []string{"caveated"}},
	{"nested groups", "view", tuple.ONR("user", "bob", "..."), []string{"nested"}},
	{"caveated nested group", "view", tuple.ONR("user", "carol", "..."), []string{"caveatednested"}},
	{"exclusion", "view_unless_banned", tuple.ONR("user", "dan", "..."), []string{"notexcluded"}},
	{"arrow over nested groups", "view_via_parent", tuple.ONR("user", "bob", "..."), []string{"infolder"}},
	{"intersection arrow", "view_via_all_groups", tuple.ONR("user", "bob", "..."), []string{"allgroups"}},
	{"fanout larger than a datastore page", "view", tuple.ONR("user", "erin", "..."), []string{"fanout"}},
}

// The legacy schema mode is not covered: it loads namespace and caveat definitions lazily, one
// at a time, including while relationship iterators are open. The single-store schema is loaded
// in full before any relationships are read, and is the supported path forward.
type poolNestingConfig struct {
	// bypassSchemaCache uses a schema hash sentinel that bypasses the stored schema cache, so that
	// every schema read goes to the datastore, as happens for requests with ZedTokens or cursors
	// that predate schema hashes.
	bypassSchemaCache bool
	concurrencyLimit  uint16
}

func (c poolNestingConfig) String() string {
	cacheMode := "cold-schema-cache"
	if c.bypassSchemaCache {
		cacheMode = "bypassed-schema-cache"
	}
	return fmt.Sprintf("%s/concurrency=%d", cacheMode, c.concurrencyLimit)
}

var poolNestingConfigs = []poolNestingConfig{
	{false, 1},
	{false, 10},
	{true, 1},
	{true, 10},
}

type poolNestingFixture struct {
	ctx        context.Context
	dispatcher dispatch.Dispatcher
	pool       *poolsim.Datastore
	revision   datastore.Revision
	schemaHash datalayer.SchemaHash
}

func (f poolNestingFixture) metadata() *v1.ResolverMeta {
	return &v1.ResolverMeta{
		AtRevision:     f.revision.String(),
		DepthRemaining: 50,
		SchemaHash:     []byte(f.schemaHash),
	}
}

func newPoolNestingFixture(t *testing.T, config poolNestingConfig, poolSize int, opts ...poolsim.Option) poolNestingFixture {
	t.Helper()

	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	// Writes the schema to both the legacy and the single-store schema tables.
	_, err = datalayer.WriteStoredSchemaForTest(t.Context(), rawDS, poolNestingSchema)
	require.NoError(t, err)

	revision, err := rawDS.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		mutations := make([]tuple.RelationshipUpdate, 0, len(poolNestingRelationships))
		for _, rel := range poolNestingRelationships {
			mutations = append(mutations, tuple.Create(rel))
		}
		return rwt.WriteRelationships(ctx, mutations)
	})
	require.NoError(t, err)

	pool := poolsim.MustNew(testfixtures.NewValidatingDatastore(rawDS), poolSize, opts...)

	// Use a real (cold) schema cache and hash, as the server does.
	schemaCache, err := cache.NewStandardCache[datalayer.SchemaCacheKey, *datastore.ReadOnlyStoredSchema](&cache.Config{
		MaxCost:    1 << 20,
		DefaultTTL: time.Minute,
	})
	require.NoError(t, err)
	t.Cleanup(schemaCache.Close)
	dlOpts := []datalayer.DataLayerOption{
		datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew),
		datalayer.WithSchemaCache(schemaCache),
	}

	schemaHash := datalayer.NoSchemaHashForTesting
	if !config.bypassSchemaCache {
		head, err := rawDS.HeadRevision(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, head.SchemaHash)
		schemaHash = datalayer.SchemaHash(head.SchemaHash)
	}

	params := MustNewDefaultDispatcherParametersForTesting()
	params.ConcurrencyLimits = SharedConcurrencyLimits(config.concurrencyLimit)
	dispatcher, err := NewLocalOnlyDispatcher(params)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dispatcher.Close() })

	ctx, cancel := context.WithTimeout(datalayer.ContextWithHandle(t.Context()), 30*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(pool, dlOpts...)))

	return poolNestingFixture{ctx, dispatcher, pool, revision, schemaHash}
}

func requireNoNestedAcquisition(t *testing.T, pool *poolsim.Datastore, err error) {
	t.Helper()
	require.NoError(t, err, "operation failed; a pooled connection was requested while one was already held")
	require.Zero(t, pool.NestedAcquisitions(), "a pooled connection was requested while one was already held")
	// Short-circuiting operations can return while cancelled branches are still unwinding.
	require.Eventually(t, func() bool { return pool.InUse() == 0 }, 5*time.Second, 10*time.Millisecond, "pooled connections were leaked")
}

func TestLookupResources3DoesNotNestPoolAcquisition(t *testing.T) {
	caveatContext, err := structpb.NewStruct(map[string]any{"flag": true})
	require.NoError(t, err)

	for _, config := range poolNestingConfigs {
		for _, tc := range poolNestingLookupCases {
			t.Run(fmt.Sprintf("%s/%s", tc.name, config), func(t *testing.T) {
				f := newPoolNestingFixture(t, config, 1)
				ctx, dispatcher, pool := f.ctx, f.dispatcher, f.pool

				stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
				err := dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
					ResourceRelation: RR("document", tc.permission).ToCoreRR(),
					SubjectRelation:  RR(tc.subject.ObjectType, tc.subject.Relation).ToCoreRR(),
					SubjectIds:       []string{tc.subject.ObjectID},
					TerminalSubject:  tc.subject.ToCoreONR(),
					Metadata:         f.metadata(),
					Context:          caveatContext,
				}, stream)
				requireNoNestedAcquisition(t, pool, err)

				// Results may contain duplicates across chunks; they are deduplicated at the API layer.
				found := mapz.NewSet[string]()
				for _, result := range stream.Results() {
					for _, item := range result.Items {
						found.Add(item.ResourceId)
					}
				}
				require.ElementsMatch(t, tc.expected, found.AsSlice())
			})
		}
	}
}

func TestLookupResources2DoesNotNestPoolAcquisition(t *testing.T) {
	caveatContext, err := structpb.NewStruct(map[string]any{"flag": true})
	require.NoError(t, err)

	for _, config := range poolNestingConfigs {
		for _, tc := range poolNestingLookupCases {
			t.Run(fmt.Sprintf("%s/%s", tc.name, config), func(t *testing.T) {
				f := newPoolNestingFixture(t, config, 1)
				ctx, dispatcher, pool := f.ctx, f.dispatcher, f.pool

				stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
				err := dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", tc.permission).ToCoreRR(),
					SubjectRelation:  RR(tc.subject.ObjectType, tc.subject.Relation).ToCoreRR(),
					SubjectIds:       []string{tc.subject.ObjectID},
					TerminalSubject:  tc.subject.ToCoreONR(),
					Metadata:         f.metadata(),
					Context:          caveatContext,
				}, stream)
				requireNoNestedAcquisition(t, pool, err)

				found := mapz.NewSet[string]()
				for _, result := range stream.Results() {
					found.Add(result.Resource.ResourceId)
				}
				require.ElementsMatch(t, tc.expected, found.AsSlice())
			})
		}
	}
}

func TestCheckDoesNotNestPoolAcquisition(t *testing.T) {
	for _, config := range poolNestingConfigs {
		for _, tc := range poolNestingLookupCases {
			for _, resourceID := range tc.expected {
				t.Run(fmt.Sprintf("%s/%s/%s", tc.name, resourceID, config), func(t *testing.T) {
					f := newPoolNestingFixture(t, config, 1)
					ctx, dispatcher, pool := f.ctx, f.dispatcher, f.pool

					resp, err := dispatcher.DispatchCheck(ctx, &v1.DispatchCheckRequest{
						ResourceRelation: RR("document", tc.permission).ToCoreRR(),
						ResourceIds:      []string{resourceID},
						ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
						Subject:          tc.subject.ToCoreONR(),
						Metadata:         f.metadata(),
					})
					requireNoNestedAcquisition(t, pool, err)
					require.Contains(t, resp.ResultsByResourceId, resourceID)
				})
			}
		}
	}
}

func TestLookupSubjectsDoesNotNestPoolAcquisition(t *testing.T) {
	for _, config := range poolNestingConfigs {
		for _, tc := range poolNestingLookupCases {
			for _, resourceID := range tc.expected {
				t.Run(fmt.Sprintf("%s/%s/%s", tc.name, resourceID, config), func(t *testing.T) {
					f := newPoolNestingFixture(t, config, 1)
					ctx, dispatcher, pool := f.ctx, f.dispatcher, f.pool

					stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
					err := dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
						ResourceRelation: RR("document", tc.permission).ToCoreRR(),
						ResourceIds:      []string{resourceID},
						SubjectRelation:  RR(tc.subject.ObjectType, tc.subject.Relation).ToCoreRR(),
						Metadata:         f.metadata(),
					}, stream)
					requireNoNestedAcquisition(t, pool, err)

					var found []string
					for _, result := range stream.Results() {
						for _, subjects := range result.FoundSubjectsByResourceId {
							for _, subject := range subjects.FoundSubjects {
								found = append(found, subject.SubjectId)
							}
						}
					}
					require.True(t, slices.Contains(found, tc.subject.ObjectID), "expected %s in %v", tc.subject.ObjectID, found)
				})
			}
		}
	}
}

func TestExpandDoesNotNestPoolAcquisition(t *testing.T) {
	for _, config := range poolNestingConfigs {
		for _, tc := range poolNestingLookupCases {
			for _, resourceID := range tc.expected {
				for _, mode := range []v1.DispatchExpandRequest_ExpansionMode{v1.DispatchExpandRequest_SHALLOW, v1.DispatchExpandRequest_RECURSIVE} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", tc.name, resourceID, mode, config), func(t *testing.T) {
						f := newPoolNestingFixture(t, config, 1)
						ctx, dispatcher, pool := f.ctx, f.dispatcher, f.pool

						_, err := dispatcher.DispatchExpand(ctx, &v1.DispatchExpandRequest{
							ResourceAndRelation: tuple.CoreONR("document", resourceID, tc.permission),
							Metadata:            f.metadata(),
							ExpansionMode:       mode,
						})
						requireNoNestedAcquisition(t, pool, err)
					})
				}
			}
		}
	}
}

// TestCaveatedLookupResourcesWithSingleConnectionPool covers the exact scenario
// reported in #3433: with a single-connection pool that blocks like a real one,
// a caveated LookupResources must complete rather than wait forever for a second
// connection while holding the first.
func TestCaveatedLookupResourcesWithSingleConnectionPool(t *testing.T) {
	caveatContext, err := structpb.NewStruct(map[string]any{"flag": true})
	require.NoError(t, err)

	f := newPoolNestingFixture(t, poolNestingConfig{false, 1}, 1,
		poolsim.WithAllowNestedAcquire(),
		poolsim.WithAcquireTimeout(2*time.Second),
	)
	ctx, dispatcher, pool := f.ctx, f.dispatcher, f.pool

	stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
	err = dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
		ResourceRelation: RR("document", "view").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"alice"},
		TerminalSubject:  tuple.CoreONR("user", "alice", "..."),
		Metadata:         f.metadata(),
		Context:          caveatContext,
	}, stream)
	require.NoError(t, err)
	require.Zero(t, pool.AcquireTimeouts())
}
