package perfinsights

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestBuildLabels(t *testing.T) {
	tcs := []struct {
		name           string
		shape          APIShapeLabels
		expectedLabels []string
	}{
		{
			name:           "empty shape",
			shape:          APIShapeLabels{},
			expectedLabels: []string{"testMethod", "", "", "", "", "", ""},
		},
		{
			name: "full shape",
			shape: APIShapeLabels{
				ResourceTypeLabel:     "resource_type",
				ResourceRelationLabel: "resource_relation",
				SubjectTypeLabel:      "subject_type",
				SubjectRelationLabel:  "subject_relation",
				NameLabel:             "name",
				FilterLabel:           "filter",
			},
			expectedLabels: []string{"testMethod", "resource_type", "resource_relation", "subject_type", "subject_relation", "name", "filter"},
		},
		{
			name: "full shape with integers",
			shape: APIShapeLabels{
				ResourceTypeLabel:     "resource_type",
				ResourceRelationLabel: "resource_relation",
				SubjectTypeLabel:      "subject_type",
				SubjectRelationLabel:  int64(40),
				NameLabel:             uint32(41),
				FilterLabel:           42,
			},
			expectedLabels: []string{"testMethod", "resource_type", "resource_relation", "subject_type", "40", "41", "42"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			labels := buildLabels("testMethod", tc.shape)
			require.Equal(t, tc.expectedLabels, labels)
		})
	}
}

func TestObserveShapeLatency(t *testing.T) {
	reg := prometheus.NewRegistry()

	metric := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                   "spicedb",
		Subsystem:                   "perf_insights",
		Name:                        "api_shape_latency_seconds",
		Help:                        "The latency of API calls, by shape",
		Buckets:                     nil,
		NativeHistogramBucketFactor: 1.1,
	}, append([]string{"api_kind"}, allLabels...))
	require.NoError(t, reg.Register(metric))

	// Report some data.
	observeShapeLatency(context.Background(), metric, "testMethod", APIShapeLabels{
		ResourceTypeLabel:     "resource_type",
		ResourceRelationLabel: "resource_relation",
		SubjectTypeLabel:      42,
	}, 100*time.Millisecond)

	// Ensure it was added to the metric.
	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metrics)

	for _, metric := range metrics {
		if !strings.HasSuffix(metric.GetName(), "api_shape_latency_seconds") {
			continue
		}

		require.Equal(t, io_prometheus_client.MetricType_HISTOGRAM, metric.GetType())

		require.Equal(t, uint64(1), metric.GetMetric()[0].Histogram.GetSampleCount())
		require.Equal(t, float64(0.1), metric.GetMetric()[0].Histogram.GetSampleSum())
		require.Equal(t, "testMethod", metric.GetMetric()[0].Label[0].GetValue())

		for _, label := range metric.GetMetric()[0].Label {
			if label.GetName() == "api_kind" {
				require.Equal(t, "testMethod", label.GetValue())
				continue
			}

			if label.GetName() == "resource_type" {
				require.Equal(t, "resource_type", label.GetValue())
				continue
			}

			if label.GetName() == "resource_relation" {
				require.Equal(t, "resource_relation", label.GetValue())
				continue
			}

			if label.GetName() == "subject_type" {
				require.Equal(t, "42", label.GetValue())
				continue
			}

			require.Empty(t, label.GetValue())
		}
	}
}
