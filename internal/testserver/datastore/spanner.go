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
func RunSpannerForTesting(t testing.TB, targetMigration string) RunningEngineForTest {
	ctx := t.Context()

	container, err := testcontainers.Run(ctx, "gcr.io/cloud-spanner-emulator/emulator:1.5.41",
	testcontainers.WithWaitStrategy(wait.ForListeningPort("9010/tcp").WithStartupTimeout(dockerBootTimeout),
	&spannerWaitStrategy{},
),
	testcontainers.WithExposedPorts("9010/tcp"),
)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	endpoint, err := container.PortEndpoint(ctx, "9010/tcp", "")
	require.NoError(t, err)

	t.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	builder := &spannerTest{
		targetMigration: targetMigration,
	}
	return builder
}

type spannerWaitStrategy struct {}

var _ wait.Strategy = (*spannerWaitStrategy)(nil)

func (s *spannerWaitStrategy) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("container not ready within the timeout: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
			instancesClient, err := instances.NewInstanceAdminClient(ctx)
			if err !=  nil {
				// If we couldn't create the client we continue
				continue
			}
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
			if err == nil {
				// If we successfully created an instance, we're done and we break
				break
			}
		}
	}
}

func (b *spannerTest) ExternalEnvVars() []string {
	return []string{fmt.Sprintf("SPANNER_EMULATOR_HOST=%s:9010", b.hostname)}
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

	migrationDriver, err := migrations.NewSpannerDriver(context.Background(), db, "", os.Getenv("SPANNER_EMULATOR_HOST"))
	require.NoError(t, err)
	defer func() {
		migrationDriver.Close(context.Background())
	}()

	err = migrations.SpannerMigrations.Run(context.Background(), migrationDriver, b.targetMigration, migrate.LiveRun)
	require.NoError(t, err)

	return initFunc("spanner", db)
}
