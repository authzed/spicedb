package graph

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func RR(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Set this to Trace to dump log statements in tests.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func TestSimpleLookup(t *testing.T) {
	testCases := []struct {
		start                 *core.RelationReference
		target                *core.ObjectAndRelation
		resolvedObjects       []*core.ObjectAndRelation
		expectedDispatchCount int
		expectedDepthRequired int
	}{
		{
			RR("document", "viewer"),
			ONR("user", "unknown", "..."),
			[]*core.ObjectAndRelation{},
			9,
			5,
		},
		{
			RR("document", "viewer"),
			ONR("user", "eng_lead", "..."),
			[]*core.ObjectAndRelation{
				ONR("document", "masterplan", "viewer"),
			},
			18,
			6,
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*core.ObjectAndRelation{
				ONR("document", "masterplan", "owner"),
			},
			2,
			2,
		},
		{
			RR("document", "viewer"),
			ONR("user", "legal", "..."),
			[]*core.ObjectAndRelation{
				ONR("document", "companyplan", "viewer"),
				ONR("document", "masterplan", "viewer"),
			},
			48,
			7,
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "multiroleguy", "..."),
			[]*core.ObjectAndRelation{
				ONR("document", "specialplan", "viewer_and_editor"),
			},
			8,
			2,
		},
		{
			RR("folder", "viewer"),
			ONR("user", "owner", "..."),
			[]*core.ObjectAndRelation{
				ONR("folder", "strategy", "viewer"),
				ONR("folder", "company", "viewer"),
			},
			33,
			6,
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"%s#%s->%s",
			tc.start.Namespace,
			tc.start.Relation,
			tuple.StringONR(tc.target),
		)

		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			ctx, dispatch, revision := newLocalDispatcher(require)

			lookupResult, err := dispatch.DispatchLookup(ctx, &v1.DispatchLookupRequest{
				ObjectRelation: tc.start,
				Subject:        tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				Limit:       10,
				DirectStack: nil,
				TtuStack:    nil,
			})

			require.NoError(err)
			require.ElementsMatch(tc.resolvedObjects, lookupResult.ResolvedOnrs)
			require.GreaterOrEqual(lookupResult.Metadata.DepthRequired, uint32(1))
			require.LessOrEqual(int(lookupResult.Metadata.DispatchCount), tc.expectedDispatchCount)
			require.Equal(0, int(lookupResult.Metadata.CachedDispatchCount))
			require.Equal(tc.expectedDepthRequired, int(lookupResult.Metadata.DepthRequired))

			// We have to sleep a while to let the cache converge:
			// https://github.com/dgraph-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
			time.Sleep(10 * time.Millisecond)

			// Run again with the cache available.
			lookupResult, err = dispatch.DispatchLookup(context.Background(), &v1.DispatchLookupRequest{
				ObjectRelation: tc.start,
				Subject:        tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				Limit:       10,
				DirectStack: nil,
				TtuStack:    nil,
			})

			require.NoError(err)
			require.ElementsMatch(tc.resolvedObjects, lookupResult.ResolvedOnrs)
			require.GreaterOrEqual(lookupResult.Metadata.DepthRequired, uint32(1))
			require.Equal(0, int(lookupResult.Metadata.DispatchCount))
			require.LessOrEqual(int(lookupResult.Metadata.CachedDispatchCount), tc.expectedDispatchCount)
			require.Equal(tc.expectedDepthRequired, int(lookupResult.Metadata.DepthRequired))
		})
	}
}

func TestMaxDepthLookup(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatch := NewLocalOnlyDispatcher()
	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))

	_, err = dispatch.DispatchLookup(ctx, &v1.DispatchLookupRequest{
		ObjectRelation: RR("document", "viewer"),
		Subject:        ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
		Limit:       10,
		DirectStack: nil,
		TtuStack:    nil,
	})

	require.Error(err)
}

type OrderedResolved []*core.ObjectAndRelation

func (a OrderedResolved) Len() int { return len(a) }

func (a OrderedResolved) Less(i, j int) bool {
	return strings.Compare(tuple.StringONR(a[i]), tuple.StringONR(a[j])) < 0
}

func (a OrderedResolved) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func TestPrefix(t *testing.T) {
	tests := []struct {
		ns         string
		wantPrefix string
	}{
		{
			ns:         "test",
			wantPrefix: "",
		},
		{
			ns:         "prefix/test",
			wantPrefix: "prefix",
		},
		{
			ns:         "prefix1/prefix2/test",
			wantPrefix: "prefix1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.ns+tt.wantPrefix, func(t *testing.T) {
			if gotPrefix := prefix(tt.ns); gotPrefix != tt.wantPrefix {
				t.Errorf("prefix() = %v, want %v", gotPrefix, tt.wantPrefix)
			}
		})
	}
}
