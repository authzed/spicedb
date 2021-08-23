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

func registerMigrateCmd(rootCmd *cobra.Command) {
	migrateCmd := &cobra.Command{
		Use:               "migrate [revision]",
		Short:             "execute datastore schema migrations",
		PersistentPreRunE: persistentPreRunE,
		Run:               migrateRun,
		Args:              cobra.ExactArgs(1),
	}

	migrateCmd.Flags().String("datastore-engine", "memory", `type of datastore to initialize ("memory", "postgres", "cockroachdb")`)
	migrateCmd.Flags().String("datastore-conn-uri", "", `connection string used by remote datastores (e.g. "postgres://postgres:password@localhost:5432/spicedb")`)

	rootCmd.AddCommand(migrateCmd)
}

func migrateRun(cmd *cobra.Command, args []string) {
	datastoreEngine := cobrautil.MustGetString(cmd, "datastore-engine")
	dbURL := cobrautil.MustGetString(cmd, "datastore-conn-uri")

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

func registerHeadCmd(rootCmd *cobra.Command) {
	headCmd := &cobra.Command{
		Use:   "head",
		Short: "compute the head database migration revision",
		Run:   headRevisionRun,
		Args:  cobra.ExactArgs(0),
	}

	headCmd.Flags().String("datastore-engine", "postgres", "type of datastore to initialize (e.g. postgres, cockroachdb, memory")

	rootCmd.AddCommand(headCmd)
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
