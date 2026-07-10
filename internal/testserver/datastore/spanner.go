package datastore

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instances "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type spannerTest struct {
	hostname        string
	port            string
	targetMigration string
}

// RunSpannerForTesting returns a RunningEngineForTest for spanner
func RunSpannerForTesting(t testing.TB, targetMigration string, opts ...testcontainers.ContainerCustomizer) RunningEngineForTest {
	ctx := t.Context()

	options := make([]testcontainers.ContainerCustomizer, 0, len(opts)+2)
	options = append(options,
		testcontainers.WithExposedPorts("9010/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9010/tcp").WithStartupTimeout(time.Minute)),
	)
	options = append(options, opts...)

	container, err := testcontainers.Run(ctx, "gcr.io/cloud-spanner-emulator/emulator:1.5.41",
		options...,
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(ctx, "9010/tcp")
	require.NoError(t, err)

	// The Spanner client libraries read SPANNER_EMULATOR_HOST, so it must be set
	// before any admin client is created below.
	t.Setenv("SPANNER_EMULATOR_HOST", net.JoinHostPort(host, mappedPort.Port()))

	// Wait until the emulator's admin API is responsive by creating an initial instance.
	require.Eventually(t, func() bool {
		instancesClient, err := instances.NewInstanceAdminClient(ctx)
		if err != nil {
			return false
		}
		defer instancesClient.Close()

		_, err = instancesClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
			Parent:     "projects/fake-project-id",
			InstanceId: "init",
			Instance: &instancepb.Instance{
				Config:      "emulator-config",
				DisplayName: "Test Instance",
				NodeCount:   1,
			},
		})
		return err == nil
	}, time.Minute, 500*time.Millisecond)

	builder := &spannerTest{
		hostname:        host,
		port:            mappedPort.Port(),
		targetMigration: targetMigration,
	}
	return builder
}

func (b *spannerTest) NewDatabase(t testing.TB) string {
	t.Logf("using spanner emulator, host: %s", os.Getenv("SPANNER_EMULATOR_HOST"))

	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newInstanceName := "fake-instance-" + uniquePortion

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	instancesClient, err := instances.NewInstanceAdminClient(ctx)
	require.NoError(t, err)
	defer instancesClient.Close()

	createInstanceOp, err := instancesClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/fake-project-id",
		InstanceId: newInstanceName,
		Instance: &instancepb.Instance{
			Config:      "emulator-config",
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	require.NoError(t, err)

	spannerInstance, err := createInstanceOp.Wait(ctx)
	require.NoError(t, err)

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	require.NoError(t, err)
	defer adminClient.Close()

	dbID := "fake-database-id"
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          spannerInstance.Name,
		CreateStatement: "CREATE DATABASE `" + dbID + "`",
	})
	require.NoError(t, err)

	db, err := op.Wait(ctx)
	require.NoError(t, err)
	return db.Name
}

func (b *spannerTest) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	db := b.NewDatabase(t)

	migrationDriver, err := migrations.NewSpannerDriver(t.Context(), db, "", os.Getenv("SPANNER_EMULATOR_HOST"))
	require.NoError(t, err)
	defer func() {
		migrationDriver.Close(t.Context())
	}()

	err = migrations.SpannerMigrations.Run(t.Context(), migrationDriver, b.targetMigration, migrate.LiveRun)
	require.NoError(t, err)

	return initFunc("spanner", db)
}
