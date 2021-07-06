package main

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/pkg/cmdutil"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

const (
	tableNamespace = "namespace_config"
	tableTuple     = "relation_tuple"

	colNamespace        = "namespace"
	colConfig           = "serialized_config"
	colDeletedTxn       = "deleted_transaction"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUsersetNamespace = "userset_namespace"
	colUsersetObjectID  = "userset_object_id"
	colUsersetRelation  = "userset_relation"

	// This is the largest positive integer possible in postgresql
	liveDeletedTxnID = uint64(9223372036854775807)
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "crdb-migration",
		Short: "Migrate tuple data to crdb.",
		PreRunE: cobrautil.CommandStack(
			cobrautil.SyncViperPreRunE("crdb-migration"),
			cobrautil.ZeroLogPreRunE,
		),
		Run: rootRun,
	}

	rootCmd.Flags().String("pg-url", "", "connection url (e.g. postgres://postgres:password@localhost:5432/spicedb) for source postgres")
	rootCmd.Flags().String("crdb-url", "", "connection url (e.g. postgres://postgres:password@localhost:26257/spicedb) for destination crdb")
	rootCmd.Flags().Bool("dry-run", true, "whether to run the migration as a dry run")

	cmdutil.RegisterLoggingPersistentFlags(rootCmd)
	cmdutil.RegisterTracingPersistentFlags(rootCmd)

	rootCmd.Execute()
}

func rootRun(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	pgUrl := cobrautil.MustGetString(cmd, "pg-url")
	if !strings.HasPrefix(pgUrl, "postgres://") {
		log.Fatal().Str("url", pgUrl).Msg("invalid source postgres url")
	}
	log.Info().Str("url", pgUrl).Msg("source postgres url")

	conn, err := pgx.Connect(ctx, pgUrl)
	if err != nil {
		log.Fatal().Err(err).Msg("error connecting to source")
	}
	defer conn.Close(ctx)

	dryRun := cobrautil.MustGetBool(cmd, "dry-run")

	var dest datastore.Datastore
	if !dryRun {
		crdbUrl := cobrautil.MustGetString(cmd, "crdb-url")
		if !strings.HasPrefix(crdbUrl, "postgres://") {
			log.Fatal().Str("url", crdbUrl).Msg("invalid destination crdb url")
		}
		log.Info().Str("url", crdbUrl).Msg("dest crdb url")

		log.Info().Msg("connecting to destination")
		dest, err = crdb.NewCRDBDatastore(crdbUrl)
		if err != nil {
			log.Fatal().Err(err).Msg("error connecting to destination")
		}
	}

	namespaces, tuples, err := prepare(ctx, conn)
	if err != nil {
		log.Fatal().Err(err).Msg("error collecting data from source")
	}

	log.Info().Int("tuples", len(tuples)).Int("namespaces", len(namespaces)).Msg("collected data from source")

	if dryRun {
		log.Fatal().Msg("dry run, aborting")
	}

	log.Info().Msg("writing data")

	err = migrate(ctx, dest, namespaces, tuples)
	if err != nil {
		log.Fatal().Err(err).Msg("error writing data to destination")
	}
}

func migrate(ctx context.Context, dest datastore.Datastore, namespaces []*v0.NamespaceDefinition, tuples []*v0.RelationTupleUpdate) error {
	for _, namespace := range namespaces {
		log.Info().Str("name", namespace.Name).Msg("writing namespace")
		if _, err := dest.WriteNamespace(ctx, namespace); err != nil {
			return err
		}
	}

	log.Info().Int("count", len(tuples)).Msg("writing all tuples")
	revision, err := dest.WriteTuples(ctx, nil, tuples)
	if err != nil {
		return err
	}

	log.Info().Stringer("timestamp", revision).Msg("tuples written")

	return nil
}

func prepare(ctx context.Context, source *pgx.Conn) ([]*v0.NamespaceDefinition, []*v0.RelationTupleUpdate, error) {
	// Collect the namespaces
	readNamespace := psql.
		Select(colNamespace, colConfig).
		From(tableNamespace).
		Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	nsSQL, nsArgs, err := readNamespace.ToSql()
	if err != nil {
		return nil, nil, err
	}

	nsRows, err := source.Query(ctx, nsSQL, nsArgs...)
	if err != nil {
		return nil, nil, err
	}

	var namespaces []*v0.NamespaceDefinition
	for nsRows.Next() {
		var config []byte
		var name string

		err := nsRows.Scan(&name, &config)
		if err != nil {
			return nil, nil, err
		}

		loaded := &v0.NamespaceDefinition{}
		err = proto.Unmarshal(config, loaded)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to unmarshal namespace (%s): %w", name, err)
		}

		namespaces = append(namespaces, loaded)

		log.Trace().Str("namespace", name).Msg("read namespace")
	}
	if err := nsRows.Err(); err != nil {
		return nil, nil, err
	}

	// Collect the tuples
	queryTuples := psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	).From(tableTuple).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	sql, args, err := queryTuples.ToSql()
	if err != nil {
		return nil, nil, err
	}

	rows, err := source.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}

	var mutations []*v0.RelationTupleUpdate
	for rows.Next() {
		nextTuple := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{},
			User: &v0.User{
				UserOneof: &v0.User_Userset{
					Userset: &v0.ObjectAndRelation{},
				},
			},
		}
		userset := nextTuple.User.GetUserset()
		err := rows.Scan(
			&nextTuple.ObjectAndRelation.Namespace,
			&nextTuple.ObjectAndRelation.ObjectId,
			&nextTuple.ObjectAndRelation.Relation,
			&userset.Namespace,
			&userset.ObjectId,
			&userset.Relation,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to scan into tuple: %w", err)
		}

		mutations = append(mutations, &v0.RelationTupleUpdate{
			Operation: v0.RelationTupleUpdate_TOUCH,
			Tuple:     nextTuple,
		})

		log.Trace().Str("tuple", tuple.String(nextTuple)).Msg("read tuple")
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return namespaces, mutations, nil
}
