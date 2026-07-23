package v1_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func TestCheckPermissionshipMetric(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	registry := prometheus.NewRegistry()
	config := testserver.DefaultTestServerConfig
	config.MetricsRegistry = registry

	conn, _, revision := testserver.NewTestServerWithConfig(t, 0, memdb.DisableGC, true, config, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)

	consistency := &v1.Consistency{
		Requirement: &v1.Consistency_AtLeastAsFresh{
			AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision, datalayer.NoSchemaHashInLegacyZedToken),
		},
	}

	// Two single checks: one allowed, one denied.
	for _, tc := range []struct {
		subjectID string
		expected  v1.CheckPermissionResponse_Permissionship
	}{
		{"eng_lead", v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION},
		{"villain", v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION},
	} {
		resp, err := client.CheckPermission(t.Context(), &v1.CheckPermissionRequest{
			Consistency: consistency,
			Resource:    obj("document", "masterplan"),
			Permission:  "view",
			Subject:     sub("user", tc.subjectID, ""),
		})
		require.NoError(t, err)
		require.Equal(t, tc.expected, resp.Permissionship)
	}

	// One bulk check with three items: two allowed, one denied.
	bulkResp, err := client.CheckBulkPermissions(t.Context(), &v1.CheckBulkPermissionsRequest{
		Consistency: consistency,
		Items: []*v1.CheckBulkPermissionsRequestItem{
			{Resource: obj("document", "masterplan"), Permission: "view", Subject: sub("user", "eng_lead", "")},
			{Resource: obj("document", "masterplan"), Permission: "view", Subject: sub("user", "owner", "")},
			{Resource: obj("document", "masterplan"), Permission: "view", Subject: sub("user", "villain", "")},
		},
	})
	require.NoError(t, err)
	require.Len(t, bulkResp.Pairs, 3)

	expectedCounts := map[[2]string]float64{
		{"CheckPermission", "HAS_PERMISSION"}:              1,
		{"CheckPermission", "NO_PERMISSION"}:               1,
		{"CheckPermission", "CONDITIONAL_PERMISSION"}:      0,
		{"CheckBulkPermissions", "HAS_PERMISSION"}:         2,
		{"CheckBulkPermissions", "NO_PERMISSION"}:          1,
		{"CheckBulkPermissions", "CONDITIONAL_PERMISSION"}: 0,
	}
	for key, expected := range expectedCounts {
		value := checkPermissionshipCounterValue(t, registry, key[0], key[1])
		require.InDelta(t, expected, value, 0, "unexpected count for method=%q permissionship=%q", key[0], key[1])
	}
}

// TestBulkImportRecordsWriteUpdatesMetric ensures that relationships written via
// ImportBulkRelationships are observed on the write-updates histogram as CREATEs.
func TestBulkImportRecordsWriteUpdatesMetric(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	registry := prometheus.NewRegistry()
	config := testserver.DefaultTestServerConfig
	config.MetricsRegistry = registry

	conn, _, _ := testserver.NewTestServerWithConfig(t, 0, memdb.DisableGC, true, config, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)

	stream, err := client.ImportBulkRelationships(t.Context())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&v1.ImportBulkRelationshipsRequest{
		Relationships: []*v1.Relationship{
			{Resource: obj("document", "importeddoc"), Relation: "viewer", Subject: sub("user", "tom", "")},
			{Resource: obj("document", "importeddoc"), Relation: "viewer", Subject: sub("user", "fred", "")},
		},
	}))
	importResp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, uint64(2), importResp.NumLoaded)

	count, sum := writeUpdatesHistogramValue(t, registry, v1.RelationshipUpdate_OPERATION_CREATE.String())
	require.Equal(t, uint64(1), count, "expected one bulk import observation")
	require.InDelta(t, float64(2), sum, 0, "expected two imported relationships observed")
}

// TestWriteRelationshipsRecordsWriteUpdatesMetric ensures that a WriteRelationships
// call is observed on the write-updates histogram, with one observation per update
// kind carrying that kind's update count.
func TestWriteRelationshipsRecordsWriteUpdatesMetric(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	registry := prometheus.NewRegistry()
	config := testserver.DefaultTestServerConfig
	config.MetricsRegistry = registry

	conn, _, _ := testserver.NewTestServerWithConfig(t, 0, memdb.DisableGC, true, config, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)

	// One write with two CREATEs, one TOUCH of an existing relationship and one
	// DELETE of an existing relationship.
	_, err := client.WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: &v1.Relationship{Resource: obj("document", "newdoc"), Relation: "viewer", Subject: sub("user", "tom", "")},
			},
			{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: &v1.Relationship{Resource: obj("document", "newdoc"), Relation: "viewer", Subject: sub("user", "fred", "")},
			},
			{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: &v1.Relationship{Resource: obj("document", "masterplan"), Relation: "viewer", Subject: sub("user", "eng_lead", "")},
			},
			{
				Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
				Relationship: &v1.Relationship{Resource: obj("document", "companyplan"), Relation: "parent", Subject: sub("folder", "company", "")},
			},
		},
	})
	require.NoError(t, err)

	for kind, expectedSum := range map[v1.RelationshipUpdate_Operation]float64{
		v1.RelationshipUpdate_OPERATION_CREATE: 2,
		v1.RelationshipUpdate_OPERATION_TOUCH:  1,
		v1.RelationshipUpdate_OPERATION_DELETE: 1,
	} {
		count, sum := writeUpdatesHistogramValue(t, registry, kind.String())
		require.Equal(t, uint64(1), count, "expected one observation for kind %s", kind)
		require.InDelta(t, expectedSum, sum, 0, "unexpected update count for kind %s", kind)
	}
}

// writeUpdatesHistogramValue reads the sample count and sum of the
// spicedb_v1_write_relationships_updates histogram for the given kind from the
// given registry.
func writeUpdatesHistogramValue(t *testing.T, registry *prometheus.Registry, kind string) (uint64, float64) {
	t.Helper()

	mfs, err := registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != "spicedb_v1_write_relationships_updates" {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "kind" && lp.GetValue() == kind {
					return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
				}
			}
		}
	}

	t.Fatalf("metric spicedb_v1_write_relationships_updates{kind=%q} not found", kind)
	return 0, 0
}

// checkPermissionshipCounterValue reads the current value of the
// spicedb_v1_check_permissionship_total counter for the given labels from the
// given registry.
func checkPermissionshipCounterValue(t *testing.T, registry *prometheus.Registry, method string, permissionship string) float64 {
	t.Helper()

	mfs, err := registry.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != "spicedb_v1_check_permissionship_total" {
			continue
		}

		for _, m := range mf.GetMetric() {
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}

			if labels["method"] == method && labels["permissionship"] == permissionship {
				return m.GetCounter().GetValue()
			}
		}
	}

	t.Fatalf("metric spicedb_v1_check_permissionship_total{method=%q,permissionship=%q} not found", method, permissionship)
	return 0
}
