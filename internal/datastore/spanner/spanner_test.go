//go:build ci
// +build ci

package spanner

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instances "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/ory/dockertest/v3"
	"github.com/rs/zerolog/log"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type spannerTest struct {
	cleanup func()
}

var spannerContainer = &dockertest.RunOptions{
	Repository: "gcr.io/cloud-spanner-emulator/emulator",
	Tag:        "latest",
}

func (st spannerTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	log.Info().Str("host", os.Getenv("SPANNER_EMULATOR_HOST")).Msg("using spanner emulator")

	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create random instance name")
	}

	newInstanceName := fmt.Sprintf("fake-instance-%s", uniquePortion)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	instancesClient, err := instances.NewInstanceAdminClient(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("error initializing instances client")
	}
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
	if err != nil {
		log.Fatal().Err(err).Msg("error creating instance")
	}

	instance, err := createInstanceOp.Wait(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("error waiting for instance")
	}

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("error initializing admin client")
	}
	defer adminClient.Close()

	dbID := "fake-database-id"
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          instance.Name,
		CreateStatement: "CREATE DATABASE `" + dbID + "`",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("error calling create database")
	}

	db, err := op.Wait(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("error waiting for database")
	}

	migrationDriver, err := migrations.NewSpannerDriver(db.Name)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	err = migrations.SpannerMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	if err != nil {
		return nil, fmt.Errorf("unable to migrate database: %w", err)
	}

	return NewSpannerDatastore(db.Name, GCWindow(gcWindow), RevisionQuantization(revisionFuzzingTimedelta))
}

func TestSpannerDatastore(t *testing.T) {
	tester := newTester(spannerContainer)
	defer tester.cleanup()

	test.All(t, tester)
}

func newTester(containerOpts *dockertest.RunOptions) *spannerTest {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatal().Err(err).Msg("could not connect to docker")
	}

	resource, err := pool.RunWithOptions(containerOpts)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not start resource")
	}

	port := resource.GetPort("9010/tcp")
	spannerEmulatorAddr := fmt.Sprintf("localhost:%s", port)
	os.Setenv("SPANNER_EMULATOR_HOST", spannerEmulatorAddr)

	if err = pool.Retry(func() error {
		ctx := context.Background()

		instancesClient, err := instances.NewInstanceAdminClient(ctx)
		if err != nil {
			return err
		}
		defer instancesClient.Close()

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
	}); err != nil {
		log.Fatal().Err(err).Msg("Could not connect to docker")
	}

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatal().Err(err).Msg("Could not purge resource")
		}
	}

	return &spannerTest{cleanup: cleanup}
}
