//go:build integration

package integrationtesting_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// TestObjectAffinityRoutingOptionReachesTestCluster checks that a cluster with routing enabled starts and serves checks.
// TestObjectDispatchClusterMatchesBaseline checks the routing end to end.
func TestObjectAffinityRoutingOptionReachesTestCluster(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)
	ds, _ := testfixtures.StandardDatastoreWithData(t, rawDS)

	conns := testserver.TestClusterWithDispatch(t, 2, ds,
		server.WithEnableExperimentalObjectAffinityRouting(true),
	)
	resp, err := v1.NewPermissionsServiceClient(conns[0]).CheckPermission(t.Context(), &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
		Resource:    &v1.ObjectReference{ObjectType: "document", ObjectId: "companyplan"},
		Permission:  "view",
		Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "auditor"}},
	})
	require.NoError(t, err)
	require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, resp.Permissionship)
}

// These metrics show that the test used object-affinity routing.
// They are on the default registry.
const (
	ownerGroupsMetric  = "spicedb_dispatch_object_owner_groups"
	ringFallbackMetric = "spicedb_dispatch_object_ring_fallback_total"

	// ownerGroupsSumKey holds the sample sum of the owner groups histogram.
	// A sum above the sample count means that a chunk went to both nodes.
	ownerGroupsSumKey = ownerGroupsMetric + "_sum"
)

var featureMetrics = []string{ownerGroupsMetric, ringFallbackMetric}

// gatherFeatureMetrics returns the value of each feature metric.
// Counters give the sum over label sets. Histograms give the sample count.
// The owner groups histogram also gives its sum under ownerGroupsSumKey.
func gatherFeatureMetrics(t *testing.T) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	values := make(map[string]float64, len(featureMetrics))
	for _, family := range families {
		if !slices.Contains(featureMetrics, family.GetName()) {
			continue
		}
		for _, m := range family.GetMetric() {
			switch {
			case m.GetCounter() != nil:
				values[family.GetName()] += m.GetCounter().GetValue()
			case m.GetHistogram() != nil:
				values[family.GetName()] += float64(m.GetHistogram().GetSampleCount())
				values[family.GetName()+"_sum"] += m.GetHistogram().GetSampleSum()
			}
		}
	}
	return values
}

// wideFanoutFolders is the number of parent folders of document:wide.
// With chunk size 2, a check of document:wide#view dispatches wideFanoutFolders/2 chunks.
// On two nodes, the probability that a chunk splits is 1 - 2^-(wideFanoutFolders/2).
const wideFanoutFolders = 60

// wideFanoutRelationships gives document:wide one parent folder for each wf-N.
// A few of the folders have viewers.
func wideFanoutRelationships() []string {
	rels := make([]string, 0, wideFanoutFolders+2)
	for i := range wideFanoutFolders {
		rels = append(rels, fmt.Sprintf("document:wide#parent@folder:wf-%d#...", i))
	}
	return append(rels,
		"folder:wf-7#viewer@user:wide_viewer_a#...",
		"folder:wf-42#editor@user:wide_editor_b#...",
	)
}

func writeRelationships(t *testing.T, ds datastore.Datastore, rels []string) datastore.Revision {
	t.Helper()
	rev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		updates := make([]tuple.RelationshipUpdate, 0, len(rels))
		for _, rel := range rels {
			updates = append(updates, tuple.Create(tuple.MustParse(rel)))
		}
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
	return rev
}

// fixtureIDs returns the sorted unique document and user IDs in rels.
func fixtureIDs(t *testing.T, rels []string) (documents, users []string) {
	t.Helper()
	docSet, userSet := map[string]struct{}{}, map[string]struct{}{}
	for _, relString := range rels {
		rel, err := tuple.Parse(relString)
		require.NoError(t, err)
		for _, onr := range []tuple.ObjectAndRelation{rel.Resource, rel.Subject} {
			switch onr.ObjectType {
			case "document":
				docSet[onr.ObjectID] = struct{}{}
			case "user":
				userSet[onr.ObjectID] = struct{}{}
			}
		}
	}
	for id := range docSet {
		documents = append(documents, id)
	}
	for id := range userSet {
		users = append(users, id)
	}
	slices.Sort(documents)
	slices.Sort(users)
	return documents, users
}

type clusterClient struct {
	client v1.PermissionsServiceClient
	token  *v1.ZedToken
}

func (c clusterClient) consistency() *v1.Consistency {
	return &v1.Consistency{Requirement: &v1.Consistency_AtExactSnapshot{AtExactSnapshot: c.token}}
}

func check(t *testing.T, c clusterClient, resource, permission, subject string) v1.CheckPermissionResponse_Permissionship {
	t.Helper()
	resp, err := c.client.CheckPermission(t.Context(), &v1.CheckPermissionRequest{
		Consistency: c.consistency(),
		Resource:    &v1.ObjectReference{ObjectType: "document", ObjectId: resource},
		Permission:  permission,
		Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: subject}},
	})
	require.NoError(t, err)
	return resp.Permissionship
}

// lookupSubjects returns sorted strings of subject ID, permissionship and excluded subject IDs.
func lookupSubjects(t *testing.T, c clusterClient, resource, permission string) []string {
	t.Helper()
	stream, err := c.client.LookupSubjects(t.Context(), &v1.LookupSubjectsRequest{
		Consistency:       c.consistency(),
		Resource:          &v1.ObjectReference{ObjectType: "document", ObjectId: resource},
		Permission:        permission,
		SubjectObjectType: "user",
	})
	require.NoError(t, err)
	var found []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		excluded := make([]string, 0, len(resp.ExcludedSubjects))
		for _, ex := range resp.ExcludedSubjects {
			excluded = append(excluded, ex.SubjectObjectId)
		}
		slices.Sort(excluded)
		found = append(found, fmt.Sprintf("%s|%s|%s",
			resp.Subject.SubjectObjectId, resp.Subject.Permissionship, strings.Join(excluded, ",")))
	}
	slices.Sort(found)
	return found
}

// TestObjectDispatchClusterMatchesBaseline compares two two-node clusters.
// The flagged cluster has object-affinity routing. The baseline cluster does not.
// Check and LookupSubjects must give the same answers.
// A wide fan-out document makes the flagged cluster split a chunk across both nodes.
//
// The feature metrics are global to the process.
// Thus the test adds only the deltas around calls to the flagged cluster.
// Top-level tests in this package run sequentially.
// Parallel subtests, as in TestConsistencyMemDB, finish before the next top-level test starts.
// The baseline cluster has routing off, so it cannot change these metrics.
func TestObjectDispatchClusterMatchesBaseline(t *testing.T) {
	newCluster := func(opts ...server.ConfigOption) clusterClient {
		rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
		require.NoError(t, err)
		ds, _ := testfixtures.StandardDatastoreWithData(t, rawDS)
		revision := writeRelationships(t, ds, wideFanoutRelationships())
		token, err := zedtoken.NewFromRevision(t.Context(), revision, "", ds)
		require.NoError(t, err)
		conns := testserver.TestClusterWithDispatch(t, 2, ds, opts...)
		return clusterClient{client: v1.NewPermissionsServiceClient(conns[0]), token: token}
	}

	baseline := newCluster(server.WithDispatchChunkSize(2))
	flagged := newCluster(
		// Small chunks cause multi-ID dispatches, so owner grouping runs.
		server.WithDispatchChunkSize(2),
		server.WithEnableExperimentalObjectAffinityRouting(true),
	)

	deltas := make(map[string]float64, len(featureMetrics))
	onFlagged := func(fn func()) {
		before := gatherFeatureMetrics(t)
		fn()
		after := gatherFeatureMetrics(t)
		for _, name := range slices.Concat(featureMetrics, []string{ownerGroupsSumKey}) {
			deltas[name] += after[name] - before[name]
		}
	}

	documents, users := fixtureIDs(t, slices.Concat(testfixtures.StandardRelationships, wideFanoutRelationships()))
	nonEmptyLookups := 0
	for _, resource := range documents {
		for _, permission := range []string{"view", "edit"} {
			for _, subject := range users {
				// The second pass uses warm caches on the flagged cluster.
				for pass := range 2 {
					want := check(t, baseline, resource, permission, subject)
					var got v1.CheckPermissionResponse_Permissionship
					onFlagged(func() { got = check(t, flagged, resource, permission, subject) })
					require.Equal(t, want, got,
						"pass %d: document:%s#%s@user:%s", pass, resource, permission, subject)
				}
			}
			wantLS := lookupSubjects(t, baseline, resource, permission)
			var gotLS []string
			onFlagged(func() { gotLS = lookupSubjects(t, flagged, resource, permission) })
			require.ElementsMatch(t, wantLS, gotLS, "lookup subjects document:%s#%s", resource, permission)
			if len(wantLS) > 0 {
				nonEmptyLookups++
			}
		}
	}

	t.Logf("flagged-cluster metric deltas: %v", deltas)
	require.Positive(t, deltas[ownerGroupsMetric], "object routing never grouped a dispatch on the flagged cluster")
	require.Greater(t, deltas[ownerGroupsSumKey], deltas[ownerGroupsMetric],
		"no dispatch on the flagged cluster split into more than one owner group")
	require.Positive(t, nonEmptyLookups, "every LookupSubjects result was empty")
}
