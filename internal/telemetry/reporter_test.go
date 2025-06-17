package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	prompb "buf.build/gen/go/prometheus/prometheus/protocolbuffers/go"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestWriteTimeSeries(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		responseBody  string
		expectedError string
		validateReq   func(t *testing.T, req *http.Request)
	}{
		{
			name:       "successful write",
			statusCode: 200,
			validateReq: func(t *testing.T, req *http.Request) {
				require.Equal(t, "POST", req.Method)
				require.Equal(t, "0.1.0", req.Header.Get("X-Prometheus-Remote-Write-Version"))
				require.Equal(t, "snappy", req.Header.Get("Content-Encoding"))
				require.Equal(t, "application/x-protobuf", req.Header.Get("Content-Type"))

				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.NotEmpty(t, body)

				// Verify the body can be decompressed
				decompressed, err := snappy.Decode(nil, body)
				require.NoError(t, err)
				require.NotEmpty(t, decompressed)
			},
		},
		{
			name:          "server error",
			statusCode:    500,
			responseBody:  "Internal Server Error",
			expectedError: "unexpected Prometheus remote write response: 500: Internal Server Error",
		},
		{
			name:          "client error",
			statusCode:    400,
			responseBody:  "Bad Request",
			expectedError: "unexpected Prometheus remote write response: 400: Bad Request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var handlerWriteErr error
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.validateReq != nil {
					tt.validateReq(t, r)
				}
				w.WriteHeader(tt.statusCode)
				if tt.responseBody != "" {
					_, handlerWriteErr = w.Write([]byte(tt.responseBody))
				}
			}))
			defer server.Close()

			ts := []*prompb.TimeSeries{
				{
					Labels: []*prompb.Label{
						{Name: "test_label", Value: "test_value"},
					},
					Samples: []*prompb.Sample{
						{Value: 42.0, Timestamp: time.Now().UnixMilli()},
					},
				},
			}

			client := &http.Client{}
			err := writeTimeSeries(t.Context(), client, server.URL, ts)

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, handlerWriteErr)
		})
	}
}

func TestDiscoverTimeseries(t *testing.T) {
	registry := prometheus.NewRegistry()

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test",
		Help: "A test gauge",
		ConstLabels: prometheus.Labels{
			"test_label": "test_value",
		},
	})
	gauge.Set(123.456)

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_total",
		Help: "A test counter",
	})
	counter.Add(42)

	registry.MustRegister(gauge, counter)

	ts, err := discoverTimeseries(registry)
	require.NoError(t, err)
	require.Len(t, ts, 2)

	for _, timeSeries := range ts {
		require.NotEmpty(t, timeSeries.Labels)
		require.Len(t, timeSeries.Samples, 1)
		require.NotZero(t, timeSeries.Samples[0].Timestamp)
	}
}

func TestDiscoverAndWriteMetrics(t *testing.T) {
	receivedData := make(chan []byte, 1)

	errChan := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		errChan <- err
		receivedData <- body
		w.WriteHeader(200)
	}))
	defer server.Close()

	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_metric",
		Help: "A test metric",
	})
	gauge.Set(100)
	registry.MustRegister(gauge)
	client := &http.Client{}
	err := discoverAndWriteMetrics(t.Context(), registry, client, server.URL)
	require.NoError(t, err)

	select {
	case data := <-receivedData:
		// Check for error from handler
		handlerErr := <-errChan
		require.NoError(t, handlerErr)
		require.NotEmpty(t, data)

		// Verify the data can be decompressed
		decompressed, err := snappy.Decode(nil, data)
		require.NoError(t, err)
		require.NotEmpty(t, decompressed)

	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for data")
	}
}

func TestRemoteReporter(t *testing.T) {
	tests := []struct {
		name          string
		endpoint      string
		interval      time.Duration
		expectedError string
	}{
		{
			name:     "valid configuration",
			endpoint: "https://example.com",
			interval: 1 * time.Minute,
		},
		{
			name:          "invalid endpoint",
			endpoint:      "://invalid-url",
			interval:      1 * time.Minute,
			expectedError: "invalid telemetry endpoint",
		},
		{
			name:          "interval too short",
			endpoint:      "https://example.com",
			interval:      30 * time.Second,
			expectedError: "invalid telemetry reporting interval",
		},
		{
			name:          "cannot change default endpoint interval",
			endpoint:      DefaultEndpoint,
			interval:      2 * time.Hour,
			expectedError: "cannot change the telemetry reporting interval for the default endpoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			reporter, err := RemoteReporter(registry, tt.endpoint, "", tt.interval)

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Nil(t, reporter)
			} else {
				require.NoError(t, err)
				require.NotNil(t, reporter)
			}
		})
	}
}

func TestRemoteReporterWithServer(t *testing.T) {
	requestCount := 0
	requestChan := make(chan bool, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		requestChan <- true
		w.WriteHeader(200)
	}))
	defer server.Close()

	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_metric",
		Help: "A test metric",
	})
	gauge.Set(42)
	registry.MustRegister(gauge)

	reporter, err := RemoteReporter(registry, server.URL, "", 1*time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()

	// Run reporter in background
	done := make(chan error, 1)
	go func() {
		done <- reporter(ctx)
	}()

	// Wait for at least one request
	select {
	case <-requestChan:
		// Success - we got a request
	case <-time.After(7 * time.Second):
		t.Fatal("timeout waiting for request")
	}

	cancel() // Stop the reporter
	<-done   // Wait for reporter to finish

	require.Positive(t, requestCount, "Expected at least one request to be made")
}

func TestRemoteReporterErrorHandling(t *testing.T) {
	failureCount := 0
	requestChan := make(chan bool, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failureCount++
		requestChan <- true
		// Return 500 to test error handling
		w.WriteHeader(500)
	}))
	defer server.Close()

	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_metric",
		Help: "A test metric",
	})
	gauge.Set(42)
	registry.MustRegister(gauge)

	reporter, err := RemoteReporter(registry, server.URL, "", 100*time.Millisecond) // Short interval for testing
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Run reporter in background
	done := make(chan error, 1)
	go func() {
		done <- reporter(ctx)
	}()

	// Wait for at least 1 request
	select {
	case <-requestChan:
		// Success - we got a request
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for first request")
	}

	cancel() // Stop the reporter
	<-done   // Wait for reporter to finish

	require.GreaterOrEqual(t, failureCount, 1, "Expected at least 1 request")
}

func TestDisabledReporter(t *testing.T) {
	err := DisabledReporter(t.Context())
	require.NoError(t, err)
}

func TestSilentlyDisabledReporter(t *testing.T) {
	err := SilentlyDisabledReporter(t.Context())
	require.NoError(t, err)
}

func TestConvertLabels(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected []*prompb.Label
	}{
		{
			name:     "empty labels",
			input:    map[string]string{},
			expected: []*prompb.Label{},
		},
		{
			name: "single label",
			input: map[string]string{
				"key": "value",
			},
			expected: []*prompb.Label{
				{Name: "key", Value: "value"},
			},
		},
		{
			name: "multiple labels",
			input: map[string]string{
				"label1": "value1",
				"label2": "value2",
			},
			expected: []*prompb.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert string map to model.Metric
			metric := make(model.Metric)
			for k, v := range tt.input {
				metric[model.LabelName(k)] = model.LabelValue(v)
			}

			result := convertLabels(metric)
			require.Len(t, result, len(tt.expected))

			// Convert to map for easier comparison since order might vary
			resultMap := make(map[string]string)
			for _, label := range result {
				resultMap[label.Name] = label.Value
			}

			expectedMap := make(map[string]string)
			for _, label := range tt.expected {
				expectedMap[label.Name] = label.Value
			}

			require.Equal(t, expectedMap, resultMap)
		})
	}
}

func TestRemoteReporterWithCustomCA(t *testing.T) {
	registry := prometheus.NewRegistry()

	// Test with non-existent CA file
	_, err := RemoteReporter(registry, "https://example.com", "/nonexistent/ca.pem", 1*time.Minute)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid custom cert pool path")
}

func TestRemoteReporterContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer server.Close()

	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_metric",
		Help: "A test metric",
	})
	gauge.Set(42)
	registry.MustRegister(gauge)

	reporter, err := RemoteReporter(registry, server.URL, "", 1*time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	err = reporter(ctx)
	require.NoError(t, err)
}

func TestWriteTimeSeriesRequestFormat(t *testing.T) {
	var capturedRequest *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRequest = r
		w.WriteHeader(200)
	}))
	defer server.Close()

	ts := []*prompb.TimeSeries{
		{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "test_metric"},
				{Name: "instance", Value: "localhost:8080"},
			},
			Samples: []*prompb.Sample{
				{Value: 1.0, Timestamp: 1234567890},
			},
		},
	}

	client := &http.Client{}
	err := writeTimeSeries(t.Context(), client, server.URL, ts)
	require.NoError(t, err)

	require.NotNil(t, capturedRequest)
	require.Equal(t, "POST", capturedRequest.Method)
	require.Equal(t, "0.1.0", capturedRequest.Header.Get("X-Prometheus-Remote-Write-Version"))
	require.Equal(t, "snappy", capturedRequest.Header.Get("Content-Encoding"))
	require.Equal(t, "application/x-protobuf", capturedRequest.Header.Get("Content-Type"))
}
