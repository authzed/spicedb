package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/tidb/migrations" // Adjusted path
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

const (
	// LatestTiDBImage is the version of TiDB to use for testing.
	// Periodically update this to the latest stable version.
	// See: https://hub.docker.com/r/pingcap/tidb/tags
	LatestTiDBImage = "v7.5.0" // Example: Use a recent stable version
	tidbPort        = "4000/tcp"
	defaultTiDBUser = "root"
	defaultTiDBPass = "" // TiDB default for root is empty password
	defaultTiDBDB   = "testdb"
)

// TiDBTesterOptions are options for running TiDB for testing.
type TiDBTesterOptions struct {
	// ImageName is the name of the TiDB Docker image to use. If empty, defaults to LatestTiDBImage.
	ImageName string

	// MigrateForNewDatastore, if true, will run migrations for a new datastore.
	MigrateForNewDatastore bool

	// ConnectionOptions are options for connecting to the TiDB instance.
	ConnectionOptions []string
}

// RunningEngineForTest represents a running datastore engine for a test.
// This is a common interface that could be used if we want to abstract
// the test engine setup further, but for now, it's implicitly used by the
// datastore-specific test runners.

// tidbSingleton is a singleton for holding the Docker-based TiDB instance.
var tidbSingleton = &sync.Once{}

// sharedTiDBInstance is the shared TiDB instance.
var sharedTiDBInstance *dockertest.Resource

// sharedTiDBURI is the URI for connecting to the shared TiDB instance.
var sharedTiDBURI string

// RunTiDBForTestingWithOptions runs a TiDB instance in Docker for testing.
func RunTiDBForTestingWithOptions(t *testing.T, options TiDBTesterOptions, dbPrefix string) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	imageName := options.ImageName
	if imageName == "" {
		imageName = LatestTiDBImage
	}

	// We only run one shared TiDB instance for all tests, to save on setup/teardown time.
	tidbSingleton.Do(func() {
		// Start TiDB
		log.Info().Str("image", imageName).Msg("starting TiDB docker container")
		resource, err := pool.RunWithOptions(&dockertest.RunOptions{
			Repository: "pingcap/tidb",
			Tag:        imageName,
			Env:        []string{}, // TiDB doesn't require specific env vars for basic setup with empty root password
			ExposedPorts: []string{tidbPort},
			PortBindings: map[docker.Port][]docker.PortBinding{
				tidbPort: {{HostIP: "0.0.0.0", HostPort: "0"}}, // Use random available host port
			},
		}, func(config *docker.HostConfig) {
			// set AutoRemove to true so that stopped container goes away by itself
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		})
		require.NoError(t, err)

		sharedTiDBInstance = resource
		hostAndPort := sharedTiDBInstance.GetHostPort(tidbPort)
		sharedTiDBURI = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", defaultTiDBUser, defaultTiDBPass, hostAndPort, defaultTiDBDB)

		log.Info().Str("uri", sharedTiDBURI).Msg("TiDB docker container started")

		// Wait for it to be ready.
		require.NoError(t, pool.Retry(func() error {
			db, err := sql.Open("mysql", sharedTiDBURI) // TiDB uses mysql driver
			if err != nil {
				log.Warn().Err(err).Msg("failed to open database connection for health check")
				return err
			}
			defer func() { _ = db.Close() }()
			return db.Ping()
		}))

		log.Info().Msg("TiDB docker container is ready")

		// If on CI, add a finalizer to clean it up.
		if os.Getenv("CI") != "" {
			t.Cleanup(func() {
				log.Info().Msg("cleaning up shared TiDB docker container")
				require.NoError(t, pool.Purge(sharedTiDBInstance))
				log.Info().Msg("cleaned up shared TiDB docker container")
			})
		}
	})

	if options.MigrateForNewDatastore {
		// Create the database if it doesn't exist.
		rootConn, err := sql.Open("mysql", strings.ReplaceAll(sharedTiDBURI, "/"+defaultTiDBDB, "/"))
		require.NoError(t, err)
		defer func() { _ = rootConn.Close() }()

		_, err = rootConn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", defaultTiDBDB))
		require.NoError(t, err)

		// Run the migrations.
		migrationDriver, err := migrations.NewTiDBDriverFromDSN(sharedTiDBURI, dbPrefix, nil)
		require.NoError(t, err)
		defer func() { _ = migrationDriver.Close(context.Background()) }()

		log.Info().Msg("running TiDB migrations")
		err = migrations.Manager.Run(context.Background(), migrationDriver, migrate.Head, migrate.LiveRun)
		require.NoError(t, err)
		log.Info().Msg("TiDB migrations completed")
	}

	return &tidbTestEngine{
		dbURI:    sharedTiDBURI,
		dbPrefix: dbPrefix,
	}
}

type tidbTestEngine struct {
	dbURI    string
	dbPrefix string
}

func (tte *tidbTestEngine) NewDatastore(t testing.TB, initFunc DatastoreInitFunc) datastore.Datastore {
	require.NotNil(t, initFunc)
	ds := initFunc("tidb", tte.dbURI) // Use string literal "tidb"
	require.NotNil(t, ds, "InitFunc returned a nil datastore")
	return ds
}
