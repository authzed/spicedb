package datastore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instances "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/google/uuid"
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
	targetMigration string
}

// RunSpannerForTesting returns a RunningEngineForTest for spanner
func RunSpannerForTesting(t testing.TB, bridgeNetworkName string, targetMigration string) RunningEngineForTest {
	ctx := context.Background()
	name := "spanner-" + uuid.New().String()

	req := testcontainers.ContainerRequest{
		Name:         name,
		Image:        "gcr.io/cloud-spanner-emulator/emulator:1.5.41",
		ExposedPorts: []string{"9010/tcp"},
		WaitingFor:   wait.ForListeningPort("9010/tcp").WithStartupTimeout(dockerBootTimeout),
	}

	if bridgeNetworkName != "" {
		req.Networks = []string{bridgeNetworkName}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	mappedPort, err := container.MappedPort(ctx, "9010")
	require.NoError(t, err)

	spannerEmulatorAddr := "localhost:" + mappedPort.Port()
	t.Setenv("SPANNER_EMULATOR_HOST", spannerEmulatorAddr)

	// Retry initialization
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), dockerBootTimeout)
		instancesClient, err := instances.NewInstanceAdminClient(ctx)
		if err == nil {
			ctx, cancel = context.WithTimeout(context.Background(), dockerBootTimeout)
			_, err = instancesClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
				Parent:     "projects/fake-project-id",
				InstanceId: "init",
				Instance: &instancepb.Instance{
					Config:      "emulator-config",
					DisplayName: "Test Instance",
					NodeCount:   1,
				},
			})
			instancesClient.Close()
			cancel()
			if err == nil {
				break
			}
		} else {
			cancel()
		}

		if i == maxRetries-1 {
			require.NoError(t, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	builder := &spannerTest{
		targetMigration: targetMigration,
	}
	if bridgeNetworkName != "" {
		builder.hostname = name
	}

	return builder
}

func (b *spannerTest) ExternalEnvVars() []string {
	return []string{fmt.Sprintf("SPANNER_EMULATOR_HOST=%s:9010", b.hostname)}
}

func (b *spannerTest) NewDatabase(t testing.TB) string {
	t.Logf("using spanner emulator, host: %s", os.Getenv("SPANNER_EMULATOR_HOST"))

	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newInstanceName := "fake-instance-" + uniquePortion

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	migrationDriver, err := migrations.NewSpannerDriver(context.Background(), db, "", os.Getenv("SPANNER_EMULATOR_HOST"))
	require.NoError(t, err)

	err = migrations.SpannerMigrations.Run(context.Background(), migrationDriver, b.targetMigration, migrate.LiveRun)
	require.NoError(t, err)

	return initFunc("spanner", db)
}
