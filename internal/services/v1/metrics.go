package v1

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// Metrics holds the Prometheus metrics exported by the v1 services. It is
// intended to be constructed once when the server is assembled and shared by
// every service registered against the same registry.
// It holds metrics that are emmitted from various APIs.
type Metrics struct {
	// checkResultCounter counts the results of CheckPermission and
	// CheckBulkPermissions calls by their returned permissionship, e.g. to
	// compute the percentage of checks that return HAS_PERMISSION.
	checkResultCounter *prometheus.CounterVec

	// writeUpdateHistogram tracks the relationship update counts per write call
	// (WriteRelationships, ImportBulkRelationships and the deprecated
	// BulkImportRelationships), by update kind.
	writeUpdateHistogram *prometheus.HistogramVec
}

// NewMetrics creates the v1 services metrics and registers them with the given
// registry. If the registry is nil, the metrics are usable but not registered
// anywhere and thus never exported; this is the safe default for tests and
// tooling that construct servers repeatedly within one process.
func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		checkResultCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "spicedb",
			Subsystem: "v1",
			Name:      "check_permissionship_total",
			Help:      "Count of CheckPermission/CheckBulkPermissions results, by API method and returned permissionship.",
		}, []string{"method", "permissionship"}),
		writeUpdateHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "spicedb",
			Subsystem: "v1",
			Name:      "write_relationships_updates",
			Help:      "The relationship update counts for the WriteRelationships and bulk import calls, by update kind.",
			// Classic buckets are kept for scrapers without native histogram support.
			// The native histogram representation has no cap.
			Buckets:                        []float64{0, 1, 2, 5, 10, 15, 25, 50, 100, 250, 500, 1000},
			NativeHistogramBucketFactor:    1.1,
			NativeHistogramMaxBucketNumber: 100,
		}, []string{"kind"}),
	}

	// Pre-initialize all label combinations so every series is exported at zero from startup
	for _, method := range []string{"CheckPermission", "CheckBulkPermissions"} {
		for name, value := range v1.CheckPermissionResponse_Permissionship_value {
			if value == int32(v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED) {
				continue
			}
			m.checkResultCounter.WithLabelValues(method, strings.TrimPrefix(name, "PERMISSIONSHIP_"))
		}
	}

	if registry != nil {
		registry.MustRegister(m.checkResultCounter, m.writeUpdateHistogram)
	}

	return m
}

// RecordCheckResult records the permissionship returned by a successful check.
func (m *Metrics) RecordCheckResult(method string, permissionship v1.CheckPermissionResponse_Permissionship) {
	m.checkResultCounter.WithLabelValues(method, strings.TrimPrefix(permissionship.String(), "PERMISSIONSHIP_")).Inc()
}

// RecordWriteRelationshipsUpdates records the number of updates of the given kind in a WriteRelationships call.
func (m *Metrics) RecordWriteRelationshipsUpdates(kind v1.RelationshipUpdate_Operation, count int) {
	m.writeUpdateHistogram.WithLabelValues(v1.RelationshipUpdate_Operation_name[int32(kind)]).Observe(float64(count))
}

// RecordBulkImportedRelationships records the number of relationships written by a bulk import call, as CREATE updates on the write-updates histogram.
func (m *Metrics) RecordBulkImportedRelationships(count uint64) {
	m.writeUpdateHistogram.WithLabelValues(v1.RelationshipUpdate_OPERATION_CREATE.String()).Observe(float64(count))
}
