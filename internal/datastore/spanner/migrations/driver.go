package migrations

import (
	"context"
	"os"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/pkg/migrate"
)

const (
	tableSchemaVersion = "schema_version"
	colVersionNum      = "version_num"
	emulatorSettingKey = "SPANNER_EMULATOR_HOST"
)

// SpannerMigrationDriver can migrate a Cloud Spanner instance
// The adminClient is required for DDL changes
type SpannerMigrationDriver struct {
	client      *spanner.Client
	adminClient *admin.DatabaseAdminClient
}

// NewSpannerDriver returns a migration driver for the given Cloud Spanner instance
func NewSpannerDriver(database, credentialsFilePath, emulatorHost string) (SpannerMigrationDriver, error) {
	ctx := context.Background()

	if len(emulatorHost) > 0 {
		os.Setenv(emulatorSettingKey, emulatorHost)
	}
	log.Info().Str("spanner-emulator-host", os.Getenv(emulatorSettingKey)).Msg("spanner emulator")
	log.Info().Str("credentials", credentialsFilePath).Str("db", database).Msg("connecting")
	client, err := spanner.NewClient(ctx, database, option.WithCredentialsFile(credentialsFilePath))
	if err != nil {
		return SpannerMigrationDriver{}, err
	}

	adminClient, err := admin.NewDatabaseAdminClient(ctx, option.WithCredentialsFile(credentialsFilePath))
	if err != nil {
		return SpannerMigrationDriver{}, err
	}

	return SpannerMigrationDriver{client, adminClient}, nil
}

func (smd SpannerMigrationDriver) Version(ctx context.Context) (string, error) {
	rows := smd.client.Single().Read(
		ctx,
		tableSchemaVersion,
		spanner.AllKeys(),
		[]string{colVersionNum},
	)
	row, err := rows.Next()
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			// There is no schema table, empty database
			return "", nil
		}
		return "", err
	}

	var schemaRevision string
	if err := row.Columns(&schemaRevision); err != nil {
		return "", err
	}

	return schemaRevision, nil
}

func (smd SpannerMigrationDriver) WriteVersion(ctx context.Context, version, replaced string) error {
	_, err := smd.client.ReadWriteTransaction(ctx, func(c context.Context, rwt *spanner.ReadWriteTransaction) error {
		return rwt.BufferWrite([]*spanner.Mutation{
			spanner.Delete(tableSchemaVersion, spanner.KeySetFromKeys(spanner.Key{replaced})),
			spanner.Insert(tableSchemaVersion, []string{colVersionNum}, []interface{}{version}),
		})
	})
	return err
}

func (smd SpannerMigrationDriver) Close() error {
	smd.client.Close()
	return nil
}

var _ migrate.Driver = SpannerMigrationDriver{}
