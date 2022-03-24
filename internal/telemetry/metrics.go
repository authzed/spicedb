package telemetry

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"runtime"

	"github.com/google/uuid"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/authzed/spicedb/internal/datastore"
)

var (
	ClusterID       = ""
	NodeID          = ""
	DatastoreEngine = ""
)

// InitializeLabels sets global values used for reporting telemetry metrics.
func InitializeLabels(dispatchUpstreamAddr, dsURI, dsEngine string) {
	ClusterID = newClusterID(dispatchUpstreamAddr, dsURI, dsEngine)
	NodeID = uuid.NewString()
	DatastoreEngine = dsEngine
}

// newClusterID creates a unique identifer for a cluster.
//
// The datastore URI is used as a private key for an SHA512_256 HMAC that
// operates on the dispatch upstream address. Memory datastores, which cannot
// cluster by definition, fallback to generating a UUID for their key.
func newClusterID(dispatchUpstreamAddr, dsURI, dsEngine string) string {
	if dsEngine == "memory" {
		dsURI = uuid.NewString()
	}

	h := hmac.New(sha512.New512_256, []byte(dsURI))
	h.Write([]byte(dispatchUpstreamAddr))
	return hex.EncodeToString(h.Sum(nil))
}

func RegisterTelemetryCollector(ds datastore.Datastore) {
	if ClusterID == "" || NodeID == "" || DatastoreEngine == "" {
		panic("register of telemetry collector before calling InitializeLabels")
	}

	prometheus.MustRegister(&collector{
		ds: ds,
		infoDesc: prometheus.NewDesc(
			prometheus.BuildFQName("spicedb", "telemetry", "info"),
			"Information about the SpiceDB environment.",
			nil,
			prometheus.Labels{
				"cluster_id": ClusterID,
				"node_id":    NodeID,
				"version":    cobrautil.Version,
				"os":         runtime.GOOS,
				"arch":       runtime.GOARCH,
				"vcpu":       fmt.Sprintf("%d", runtime.NumCPU()),
				"ds_engine":  DatastoreEngine,
			},
		),
		objectDefsDesc: prometheus.NewDesc(
			prometheus.BuildFQName("spicedb", "telemetry", "object_definitions_total"),
			"Count of the number of objects defined by the schema.",
			nil,
			prometheus.Labels{
				"cluster_id": ClusterID,
				"node_id":    NodeID,
			},
		),
	})
}

type collector struct {
	ds             datastore.Datastore
	infoDesc       *prometheus.Desc
	objectDefsDesc *prometheus.Desc
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.infoDesc
	ch <- c.objectDefsDesc
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.infoDesc, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.objectDefsDesc, prometheus.GaugeValue, countObjectDefs(c.ds))
}

func countObjectDefs(ds datastore.Datastore) float64 {
	ctx := context.Background()
	rev, err := ds.OptimizedRevision(ctx)
	if err != nil {
		return -1
	}

	namespaces, err := ds.ListNamespaces(ctx, rev)
	if err != nil {
		return -1
	}

	return float64(len(namespaces))
}
