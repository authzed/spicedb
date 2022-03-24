// Package telemetry implements a client for reporting telemetry data used to
// prioritize development of SpiceDB.
//
// For more information, see:
// https://github.com/authzed/spicedb/blob/main/TELEMETRY.md
package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
)

const (
	DefaultEndpoint = "https://telemetry.authzed.com"
	MetricPrefix    = "spicedb_telemetry_"
	Interval        = time.Hour
)

func writeTimeSeries(ctx context.Context, client *http.Client, endpoint string, ts []*prompb.TimeSeries) error {
	pbBytes, err := proto.Marshal(&prompb.WriteRequest{
		Timeseries: ts,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal Prometheus remote write protobuf: %w", err)
	}
	compressedPB := snappy.Encode(nil, pbBytes)

	r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(compressedPB))
	if err != nil {
		return fmt.Errorf("failed to create Prometheus remote write http request: %w", err)
	}

	r.Header.Add("X-Prometheus-Remote-Write-Version", "0.1.0")
	r.Header.Add("Content-Encoding", "snappy")
	r.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := client.Do(r)
	if err != nil {
		return fmt.Errorf("failed to send Prometheus remote write: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf(
			"unexpected Prometheus remote write response: %d: %s",
			resp.StatusCode,
			string(body),
		)
	}

	return nil
}

func timestamp(m *dto.Metric) int64 {
	if ts := m.GetTimestampMs(); ts != 0 {
		return ts
	}
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func nameLabel(name string) *prompb.Label {
	return &prompb.Label{
		Name:  model.MetricNameLabel,
		Value: name,
	}
}

func discoverTimeseries(include func(*dto.MetricFamily) bool) (allTS []*prompb.TimeSeries, err error) {
	metricFams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil, fmt.Errorf("failed to gather telemetry metrics: %w", err)
	}

	for _, fam := range metricFams {
		if include != nil && !include(fam) {
			continue
		}

		for _, metric := range fam.GetMetric() {
			var labels []*prompb.Label
			for _, label := range metric.GetLabel() {
				labels = append(labels, &prompb.Label{
					Name:  label.GetName(),
					Value: label.GetValue(),
				})
			}

			switch *fam.Type {
			case dto.MetricType_COUNTER:
				allTS = append(allTS, &prompb.TimeSeries{
					Labels: append(labels, nameLabel(fam.GetName())),
					Samples: []prompb.Sample{{
						Timestamp: timestamp(metric),
						Value:     metric.GetCounter().GetValue(),
					}},
				})
			case dto.MetricType_GAUGE:
				allTS = append(allTS, &prompb.TimeSeries{
					Labels: append(labels, nameLabel(fam.GetName())),
					Samples: []prompb.Sample{{
						Timestamp: timestamp(metric),
						Value:     metric.GetGauge().GetValue(),
					}},
				})
			case dto.MetricType_HISTOGRAM:
				histogram := metric.GetHistogram()

				allTS = append(
					allTS,
					&prompb.TimeSeries{
						Labels: append(labels, nameLabel(fam.GetName()+"_sum")),
						Samples: []prompb.Sample{{
							Timestamp: timestamp(metric),
							Value:     histogram.GetSampleSum(),
						}},
					}, &prompb.TimeSeries{
						Labels: append(labels, nameLabel(fam.GetName()+"_count")),
						Samples: []prompb.Sample{{
							Timestamp: timestamp(metric),
							Value:     float64(histogram.GetSampleCount()),
						}},
					},
				)

				for _, bucket := range histogram.GetBucket() {
					allTS = append(allTS, &prompb.TimeSeries{
						Labels: append(labels, nameLabel(fam.GetName()+"_bucket"), &prompb.Label{
							Name:  model.BucketLabel,
							Value: strconv.FormatFloat(bucket.GetUpperBound(), 'f', -1, 64),
						}),
						Samples: []prompb.Sample{{
							Timestamp: timestamp(metric),
							Value:     float64(bucket.GetCumulativeCount()),
						}},
					})
				}
			default:
				panic(fmt.Sprintf("unsupported metric type: %s", fam.Type))
			}
		}
	}

	return
}

func onlyTelemetryMetrics(fam *dto.MetricFamily) bool {
	return strings.HasPrefix(fam.GetName(), MetricPrefix)
}

func discoverAndWriteMetrics(ctx context.Context, endpoint string) error {
	ts, err := discoverTimeseries(onlyTelemetryMetrics)
	if err != nil {
		return err
	}

	if endpoint == "" || len(ts) == 0 {
		return nil
	}

	return writeTimeSeries(ctx, &http.Client{}, endpoint, ts)
}

// ValidateEndpoint returns an error if the provided endpoint cannot be used
// to report telemetry.
func ValidateEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}

	if _, err := url.Parse(endpoint); err != nil {
		return fmt.Errorf("invalid telemetry endpoint: %w", err)
	}
	return nil
}

// ReportForever reports telemetry to the endpoint until the context is
// canceled while warning on failure.
func ReportForever(ctx context.Context, endpoint string) error {
	if err := ValidateEndpoint(endpoint); err != nil {
		return err
	}

	log.Info().
		Dur("interval", Interval).
		Str("endpoint", endpoint).
		Msg("telemetry reporter scheduled")

	// Fire off the first at start-up.
	if err := discoverAndWriteMetrics(ctx, endpoint); err != nil {
		log.Warn().
			Err(err).
			Str("endpoint", endpoint).
			Msg("failed to push telemetry metric")
	}

	backoffInterval := backoff.NewExponentialBackOff()
	backoffInterval.InitialInterval = Interval
	ticker := time.After(backoffInterval.InitialInterval)

	for {
		select {
		case <-ticker:
			nextPush := backoffInterval.InitialInterval
			if err := discoverAndWriteMetrics(ctx, endpoint); err != nil {
				log.Warn().
					Err(err).
					Str("endpoint", endpoint).
					Msg("failed to push telemetry metric")
				nextPush = backoffInterval.NextBackOff()
			} else {
				backoffInterval.Reset()
			}
			ticker = time.After(nextPush)

		case <-ctx.Done():
			return nil
		}
	}
}
