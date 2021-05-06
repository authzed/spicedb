package main

import (
	"strings"

	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
)

func migrateRun(cmd *cobra.Command, args []string) {
	dbURL := cobrautil.MustGetString(cmd, "datastore-url")
	if !strings.HasPrefix(dbURL, "postgres://") {
		log.Fatal().Msg("must run migrations on postgres databases")
	}

	migrationDriver, err := migrations.NewAlembicPostgresDriver(dbURL)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to create migration driver")
	}

	targetRevision := args[0]

	log.Info().Str("targetRevision", targetRevision).Msg("running migrations")
	err = migrations.DatabaseMigrations.Run(migrationDriver, targetRevision, migrate.LiveRun)
	if err != nil {
		log.Fatal().Msg("unable to complete request migrations")
	}
}
