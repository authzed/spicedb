package graph

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

func RR(namespaceName string, relationName string) *pb.RelationReference {
	return &pb.RelationReference{
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
		start           *pb.RelationReference
		target          *pb.ObjectAndRelation
		resolvedObjects []*pb.ObjectAndRelation
	}{
		{
			RR("document", "viewer"),
			ONR("user", "unknown", "..."),
			[]*pb.ObjectAndRelation{},
		},
		{
			RR("document", "viewer"),
			ONR("user", "eng_lead", "..."),
			[]*pb.ObjectAndRelation{
				ONR("document", "masterplan", "viewer"),
			},
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*pb.ObjectAndRelation{
				ONR("document", "masterplan", "owner"),
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "legal", "..."),
			[]*pb.ObjectAndRelation{
				ONR("document", "companyplan", "viewer"),
				ONR("document", "masterplan", "viewer"),
			},
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "multiroleguy", "..."),
			[]*pb.ObjectAndRelation{
				ONR("document", "specialplan", "viewer_and_editor"),
			},
		},
		{
			RR("folder", "viewer"),
			ONR("user", "owner", "..."),
			[]*pb.ObjectAndRelation{
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

			lookupResult := dispatch.Lookup(context.Background(), LookupRequest{
				StartRelation:  tc.start,
				TargetONR:      tc.target,
				AtRevision:     revision,
				DepthRemaining: 50,
				Limit:          10,
				DirectStack:    tuple.NewONRSet(),
				TTUStack:       tuple.NewONRSet(),
				DebugTracer:    NewNullTracer(),
			})

			sort.Sort(OrderedResolved(tc.resolvedObjects))
			sort.Sort(OrderedResolved(lookupResult.ResolvedObjects))

			require.Equal(tc.resolvedObjects, lookupResult.ResolvedObjects)
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

	lookupResult := dispatch.Lookup(context.Background(), LookupRequest{
		StartRelation:  RR("document", "viewer"),
		TargetONR:      ONR("user", "legal", "..."),
		AtRevision:     revision,
		DepthRemaining: 0,
		Limit:          10,
		DirectStack:    tuple.NewONRSet(),
		TTUStack:       tuple.NewONRSet(),
		DebugTracer:    NewNullTracer(),
	})

	require.Error(lookupResult.Err)
}

type OrderedResolved []*pb.ObjectAndRelation

func (a OrderedResolved) Len() int { return len(a) }

func (a OrderedResolved) Less(i, j int) bool {
	return strings.Compare(tuple.StringONR(a[i]), tuple.StringONR(a[j])) < 0
}

func (a OrderedResolved) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
