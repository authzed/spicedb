// Package telemetry implements a client for reporting telemetry data used to
// prioritize development of SpiceDB.
//
// For more information, see:
// https://github.com/authzed/spicedb/blob/main/TELEMETRY.md
package telemetry

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	prompb "buf.build/gen/go/prometheus/prometheus/protocolbuffers/go"
	"github.com/cenkalti/backoff/v4"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/x509util"
)

const (
	// DefaultEndpoint is the endpoint to which telemetry will report if none
	// other is specified.
	DefaultEndpoint = "https://telemetry.authzed.com"

	// DefaultInterval is the default amount of time to wait between telemetry
	// reports.
	DefaultInterval = 1 * time.Hour

	// MaxElapsedTimeBetweenReports is the maximum amount of time that the
	// telemetry reporter will attempt to write to the telemetry endpoint
	// before terminating the reporter.
	MaxElapsedTimeBetweenReports = 168 * time.Hour

	// MinimumAllowedInterval is the minimum amount of time one can request
	// between telemetry reports.
	MinimumAllowedInterval = 1 * time.Minute
)

func writeTimeSeries(ctx context.Context, client *http.Client, endpoint string, ts []*prompb.TimeSeries) error {
	// Reference upstream client:
	// https://github.com/prometheus/prometheus/blob/6555cc68caf8d8f323056e497ae7bb1e32a81667/storage/remote/client.go#L191
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
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf(
			"unexpected Prometheus remote write response: %d: %s",
			resp.StatusCode,
			string(body),
		)
	}

	return nil
}

func discoverTimeseries(registry *prometheus.Registry) (allTS []*prompb.TimeSeries, err error) {
	metricFams, err := registry.Gather()
	if err != nil {
		return nil, fmt.Errorf("failed to gather telemetry metrics: %w", err)
	}

	defaultTimestamp := model.Time(time.Now().UnixNano() / int64(time.Millisecond))
	sampleVector, err := expfmt.ExtractSamples(&expfmt.DecodeOptions{
		Timestamp: defaultTimestamp,
	}, metricFams...)
	if err != nil {
		return nil, fmt.Errorf("unable to extract sample from metrics families: %w", err)
	}

	for _, sample := range sampleVector {
		allTS = append(allTS, &prompb.TimeSeries{
			Labels: convertLabels(sample.Metric),
			Samples: []*prompb.Sample{{
				Value:     float64(sample.Value),
				Timestamp: int64(sample.Timestamp),
			}},
		})
	}

	return
}

func discoverAndWriteMetrics(
	ctx context.Context,
	registry *prometheus.Registry,
	client *http.Client,
	endpoint string,
) error {
	ts, err := discoverTimeseries(registry)
	if err != nil {
		return err
	}

	return writeTimeSeries(ctx, client, endpoint, ts)
}

type Reporter func(ctx context.Context) error

// RemoteReporter creates a telemetry reporter with the specified parameters, or errors
// if the configuration was invalid.
func RemoteReporter(
	registry *prometheus.Registry,
	endpoint string,
	caOverridePath string,
	interval time.Duration,
) (Reporter, error) {
	if _, err := url.Parse(endpoint); err != nil {
		return nil, fmt.Errorf("invalid telemetry endpoint: %w", err)
	}
	if interval < MinimumAllowedInterval {
		return nil, fmt.Errorf("invalid telemetry reporting interval: %s < %s", interval, MinimumAllowedInterval)
	}
	if endpoint == DefaultEndpoint && interval != DefaultInterval {
		return nil, fmt.Errorf("cannot change the telemetry reporting interval for the default endpoint")
	}

	client := &http.Client{}
	if caOverridePath != "" {
		pool, err := x509util.CustomCertPool(caOverridePath)
		if err != nil {
			return nil, fmt.Errorf("invalid custom cert pool path `%s`: %w", caOverridePath, err)
		}

		t := &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
			},
		}

		client.Transport = t
	}

	return func(ctx context.Context) error {
		// Smear the startup delay out over 10% of the reporting interval
		startupDelay := time.Duration(rand.Int63n(int64(interval.Seconds()/10))) * time.Second

		log.Ctx(ctx).Info().
			Stringer("interval", interval).
			Str("endpoint", endpoint).
			Stringer("next", startupDelay).
			Msg("telemetry reporter scheduled")

		backoffInterval := backoff.NewExponentialBackOff()
		backoffInterval.InitialInterval = interval
		backoffInterval.MaxInterval = MaxElapsedTimeBetweenReports
		backoffInterval.MaxElapsedTime = 0

		// Must reset the backoff object after changing parameters
		backoffInterval.Reset()

		ticker := time.After(startupDelay)

		for {
			select {
			case <-ticker:
				nextPush := backoffInterval.InitialInterval
				if err := discoverAndWriteMetrics(ctx, registry, client, endpoint); err != nil {
					nextPush = backoffInterval.NextBackOff()
					log.Ctx(ctx).Warn().
						Err(err).
						Str("endpoint", endpoint).
						Stringer("next", nextPush).
						Msg("failed to push telemetry metric")
				} else {
					log.Ctx(ctx).Debug().
						Str("endpoint", endpoint).
						Stringer("next", nextPush).
						Msg("reported telemetry")
					backoffInterval.Reset()
				}
				if nextPush == backoff.Stop {
					return fmt.Errorf(
						"exceeded maximum time between successful reports of %s",
						MaxElapsedTimeBetweenReports,
					)
				}
				ticker = time.After(nextPush)

			case <-ctx.Done():
				return nil
			}
		}
	}, nil
}

func DisabledReporter(ctx context.Context) error {
	log.Ctx(ctx).Info().Msg("telemetry disabled")
	return nil
}

func SilentlyDisabledReporter(_ context.Context) error {
	return nil
}

func convertLabels(labels model.Metric) []*prompb.Label {
	out := make([]*prompb.Label, 0, len(labels))
	for name, value := range labels {
		out = append(out, &prompb.Label{
			Name:  string(name),
			Value: string(value),
		})
	}
	return out
}
