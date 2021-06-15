package main

import (
	"fmt"

	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
)

func migrateRun(cmd *cobra.Command, args []string) {
	datastoreEngine := cobrautil.MustGetString(cmd, "datastore-engine")
	dbURL := cobrautil.MustGetString(cmd, "datastore-url")

	if datastoreEngine == "cockroachdb" {
		log.Info().Msg("migrating cockroachdb datastore")

		migrationDriver, err := crdbmigrations.NewCRDBDriver(dbURL)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}

		targetRevision := args[0]

		log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
		err = crdbmigrations.CRDBMigrations.Run(migrationDriver, targetRevision, migrate.LiveRun)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to complete requested migrations")
		}
	} else if datastoreEngine == "postgres" {
		log.Info().Msg("migrating postgres datastore")
		migrationDriver, err := migrations.NewAlembicPostgresDriver(dbURL)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to create migration driver")
		}

		targetRevision := args[0]

		log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
		err = migrations.DatabaseMigrations.Run(migrationDriver, targetRevision, migrate.LiveRun)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to complete requested migrations")
		}
	} else {
		log.Fatal().Str("datastore-engine", datastoreEngine).Msg("cannot migrate datastore engine type")
	}
}

func headRevisionRun(cmd *cobra.Command, args []string) {
	datastoreEngine := cobrautil.MustGetString(cmd, "datastore-engine")

	var headRevision string
	var err error

	if datastoreEngine == "cockroachdb" {
		headRevision, err = crdbmigrations.CRDBMigrations.HeadRevision()
	} else if datastoreEngine == "postgres" {
		headRevision, err = migrations.DatabaseMigrations.HeadRevision()
	} else {
		log.Fatal().Str("datastore-engine", datastoreEngine).Msg("cannot migrate datastore engine type")
	}

	if err != nil {
		log.Fatal().Err(err).Msg("unable to compute head revision")
	}

	fmt.Println(headRevision)
}
