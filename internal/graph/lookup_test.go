package graph

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

func RR(namespaceName string, relationName string) *v0.RelationReference {
	return &v0.RelationReference{
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
		start           *v0.RelationReference
		target          *v0.ObjectAndRelation
		resolvedObjects []*v0.ObjectAndRelation
	}{
		{
			RR("document", "viewer"),
			ONR("user", "unknown", "..."),
			[]*v0.ObjectAndRelation{},
		},
		{
			RR("document", "viewer"),
			ONR("user", "eng_lead", "..."),
			[]*v0.ObjectAndRelation{
				ONR("document", "masterplan", "viewer"),
			},
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*v0.ObjectAndRelation{
				ONR("document", "masterplan", "owner"),
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "legal", "..."),
			[]*v0.ObjectAndRelation{
				ONR("document", "companyplan", "viewer"),
				ONR("document", "masterplan", "viewer"),
			},
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "multiroleguy", "..."),
			[]*v0.ObjectAndRelation{
				ONR("document", "specialplan", "viewer_and_editor"),
			},
		},
		{
			RR("folder", "viewer"),
			ONR("user", "owner", "..."),
			[]*v0.ObjectAndRelation{
				ONR("folder", "strategy", "viewer"),
				ONR("folder", "company", "viewer"),
			},
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

			dispatch, revision := newLocalDispatcher(require)

			lookupResult := dispatch.DispatchLookup(context.Background(), &v1.DispatchLookupRequest{
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

			require.NoError(lookupResult.Err)
			require.ElementsMatch(tc.resolvedObjects, lookupResult.Resp.ResolvedOnrs)
		})
	}
}

func TestMaxDepthLookup(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	nsm, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, testCacheConfig)
	require.NoError(err)

	dispatch, err := NewLocalDispatcher(nsm, ds)
	require.NoError(err)

	lookupResult := dispatch.DispatchLookup(context.Background(), &v1.DispatchLookupRequest{
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

	require.Error(lookupResult.Err)
}

type OrderedResolved []*v0.ObjectAndRelation

func (a OrderedResolved) Len() int { return len(a) }

func (a OrderedResolved) Less(i, j int) bool {
	return strings.Compare(tuple.StringONR(a[i]), tuple.StringONR(a[j])) < 0
}

func (a OrderedResolved) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
