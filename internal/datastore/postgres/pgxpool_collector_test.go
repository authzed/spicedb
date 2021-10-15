//go:build ci
// +build ci

package postgres

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/prometheus/client_golang/prometheus"
)

// Based on https://github.com/prometheus/client_golang/blob/master/prometheus/collectors/dbstats_collector_test.go
func TestPgxpoolStatsCollector(t *testing.T) {
	pgContainer := &dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "9.6",
		Env:        []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=defaultdb"},
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(pgContainer)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	var dbpool *pgxpool.Pool
	port := resource.GetPort(fmt.Sprintf("%d/tcp", 5432))
	if err = pool.Retry(func() error {
		var err error
		dbpool, err = pgxpool.Connect(context.Background(), fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", "postgres:secret", port))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
	defer cleanup()

	// Test registering collector
	reg := prometheus.NewRegistry()
	{
		if err := reg.Register(NewPgxpoolStatsCollector(dbpool, "db_A")); err != nil {
			t.Fatal(err)
		}
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	// Expected metric names
	names := []string{
		"pgxpool_db_acquire_count",
		"pgxpool_db_acquire_duration",
		"pgxpool_db_acquire_conns",
		"pgxpool_db_canceled_acquire_count",
		"pgxpool_db_constructing_conns",
		"pgxpool_db_empty_acquire_count",
		"pgxpool_db_idle_conns",
		"pgxpool_db_max_conns",
		"pgxpool_db_total_conns",
	}
	type result struct {
		found bool
	}
	results := make(map[string]result)
	for _, name := range names {
		results[name] = result{found: false}
	}
	for _, mf := range mfs {
		m := mf.GetMetric()
		if len(m) != 1 {
			t.Errorf("expected 1 metric but got %d", len(m))
		}
		labelA := m[0].GetLabel()[0]
		if name := labelA.GetName(); name != "db_name" {
			t.Errorf("expected to get label \"db_name\" but got %s", name)
		}
		if value := labelA.GetValue(); value != "db_A" {
			t.Errorf("expected to get value \"db_A\" but got %s", value)
		}

		for _, name := range names {
			if name == mf.GetName() {
				results[name] = result{found: true}
				break
			}
		}
	}

	for name, result := range results {
		if !result.found {
			t.Errorf("%s not found", name)
		}
	}
}
