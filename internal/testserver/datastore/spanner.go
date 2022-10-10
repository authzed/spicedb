package datastore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instances "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type spannerTest struct {
	hostname string
}

// RunSpannerForTesting returns a RunningEngineForTest for spanner
func RunSpannerForTesting(t testing.TB, bridgeNetworkName string) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	name := fmt.Sprintf("postgres-%s", uuid.New().String())
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         name,
		Repository:   "gcr.io/cloud-spanner-emulator/emulator",
		Tag:          "latest",
		ExposedPorts: []string{"9010/tcp"},
		NetworkID:    bridgeNetworkName,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort("9010/tcp")
	spannerEmulatorAddr := fmt.Sprintf("localhost:%s", port)
	require.NoError(t, os.Setenv("SPANNER_EMULATOR_HOST", spannerEmulatorAddr))

	require.NoError(t, pool.Retry(func() error {
		ctx := context.Background()

		instancesClient, err := instances.NewInstanceAdminClient(ctx)
		if err != nil {
			return err
		}
		defer func() { require.NoError(t, instancesClient.Close()) }()

		_, err = instancesClient.CreateInstance(ctx, &instance.CreateInstanceRequest{
			Parent:     "projects/fake-project-id",
			InstanceId: "init",
			Instance: &instance.Instance{
				Config:      "emulator-config",
				DisplayName: "Test Instance",
				NodeCount:   1,
			},
		})
		return err
	}))

	builder := &spannerTest{}
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

	newInstanceName := fmt.Sprintf("fake-instance-%s", uniquePortion)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	instancesClient, err := instances.NewInstanceAdminClient(ctx)
	require.NoError(t, err)
	defer instancesClient.Close()

	createInstanceOp, err := instancesClient.CreateInstance(ctx, &instance.CreateInstanceRequest{
		Parent:     "projects/fake-project-id",
		InstanceId: newInstanceName,
		Instance: &instance.Instance{
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

	migrationDriver, err := migrations.NewSpannerDriver(db, "", os.Getenv("SPANNER_EMULATOR_HOST"))
	require.NoError(t, err)

	err = migrations.SpannerMigrations.Run(context.Background(), migrationDriver, migrate.Head, migrate.LiveRun)
	require.NoError(t, err)

	return initFunc("spanner", db)
}
