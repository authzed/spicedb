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
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
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
		relation        *pb.RelationReference
		start           *pb.ObjectAndRelation
		isRootRequest   bool
		resolvedObjects []ResolvedObject
	}{
		{
			RR("document", "viewer"),
			ONR("user", "unknown", "..."),
			true,
			[]ResolvedObject{},
		},
		{
			RR("document", "viewer"),
			ONR("user", "eng_lead", "..."),
			true,
			[]ResolvedObject{
				ResolvedObject{ONR("document", "masterplan", "viewer"), ""},
			},
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			true,
			[]ResolvedObject{
				ResolvedObject{ONR("document", "masterplan", "owner"), ""},
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "legal", "..."),
			true,
			[]ResolvedObject{
				ResolvedObject{ONR("document", "companyplan", "viewer"), ""},
				ResolvedObject{ONR("document", "masterplan", "viewer"), ""},
			},
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "multiroleguy", "..."),
			true,
			[]ResolvedObject{
				ResolvedObject{ONR("document", "specialplan", "viewer_and_editor"), ""},
			},
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "multiroleguy", "..."),
			false,
			[]ResolvedObject{
				ResolvedObject{
					ONR:             ONR("document", "specialplan", "viewer_and_editor"),
					ReductionNodeID: "document#viewer_and_editor::3",
				},
				ResolvedObject{
					ONR:             ONR("document", "specialplan", "viewer_and_editor"),
					ReductionNodeID: "document#viewer_and_editor::4",
				},
			},
		},
		{
			RR("folder", "viewer"),
			ONR("user", "owner", "..."),
			true,
			[]ResolvedObject{
				ResolvedObject{ONR("folder", "strategy", "viewer"), ""},
				ResolvedObject{ONR("folder", "company", "viewer"), ""},
			},
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"%s->%s#%s::%v",
			tuple.StringONR(tc.start),
			tc.relation.Namespace,
			tc.relation.Relation,
			tc.isRootRequest,
		)

		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			dispatch, revision := newLocalDispatcher(require)

			lookupResult := dispatch.Lookup(context.Background(), LookupRequest{
				Start:          tc.start,
				TargetRelation: tc.relation,
				IsRootRequest:  tc.isRootRequest,
				AtRevision:     revision,
				DepthRemaining: 50,
				Limit:          10,
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
		Start:          ONR("user", "legal", "..."),
		TargetRelation: RR("document", "viewer"),
		IsRootRequest:  true,
		AtRevision:     revision,
		DepthRemaining: 1,
		Limit:          10,
	})

	require.Error(lookupResult.Err)
}

type OrderedResolved []ResolvedObject

func (a OrderedResolved) Len() int { return len(a) }

func (a OrderedResolved) Less(i, j int) bool {
	result := strings.Compare(tuple.StringONR(a[i].ONR), tuple.StringONR(a[j].ONR))
	if result < 0 {
		return false
	}

	if result > 0 {
		return true
	}

	return strings.Compare(string(a[i].ReductionNodeID), string(a[j].ReductionNodeID)) < 0
}

func (a OrderedResolved) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
