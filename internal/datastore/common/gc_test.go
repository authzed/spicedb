package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"

	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type testGC struct{}

func (t testGC) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{}, fmt.Errorf("hi")
}

func (t testGC) Now(_ context.Context) (time.Time, error) {
	return time.Now(), fmt.Errorf("hi")
}

func (t testGC) TxIDBefore(_ context.Context, _ time.Time) (datastore.Revision, error) {
	return nil, fmt.Errorf("hi")
}

func (t testGC) DeleteBeforeTx(_ context.Context, _ datastore.Revision) (DeletionCounts, error) {
	return DeletionCounts{}, fmt.Errorf("hi")
}

func TestGCFailureBackoff(t *testing.T) {
	defer func() {
		gcFailureCounter = prometheus.NewCounter(gcFailureCounterConfig)
	}()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(gcFailureCounter))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		require.Error(t, startGarbageCollectorWithMaxElapsedTime(ctx, testGC{}, 100*time.Millisecond, 1*time.Second, 1*time.Nanosecond, 1*time.Minute))
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()

	metrics, err := reg.Gather()
	require.NoError(t, err)
	var mf *promclient.MetricFamily
	for _, metric := range metrics {
		if metric.GetName() == "spicedb_datastore_gc_failure_total" {
			mf = metric
		}
	}
	require.Greater(t, *(mf.GetMetric()[0].Counter.Value), 100.0, "MaxElapsedTime=1ns did not cause backoff to get ignored")

	gcFailureCounter = prometheus.NewCounter(gcFailureCounterConfig)
	reg = prometheus.NewRegistry()
	require.NoError(t, reg.Register(gcFailureCounter))
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() {
		require.Error(t, StartGarbageCollector(ctx, testGC{}, 100*time.Millisecond, 1*time.Second, 1*time.Minute))
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()

	metrics, err = reg.Gather()
	require.NoError(t, err)
	for _, metric := range metrics {
		if metric.GetName() == "spicedb_datastore_gc_failure_total" {
			mf = metric
		}
	}
	require.Less(t, *(mf.GetMetric()[0].Counter.Value), 3.0, "MaxElapsedTime=0 should have not caused backoff to get ignored")
}
