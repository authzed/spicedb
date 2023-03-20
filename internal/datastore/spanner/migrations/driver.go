package migrations

import (
	"context"
	"os"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	log "github.com/authzed/spicedb/internal/logging"
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

// Wrapper makes it possible to forward the spanner clients to the MigrationFunc's to execute
type Wrapper struct {
	client      *spanner.Client
	adminClient *admin.DatabaseAdminClient
}

// NewSpannerDriver returns a migration driver for the given Cloud Spanner instance
func NewSpannerDriver(database, credentialsFilePath, emulatorHost string) (*SpannerMigrationDriver, error) {
	ctx := context.Background()

	if len(emulatorHost) > 0 {
		err := os.Setenv(emulatorSettingKey, emulatorHost)
		if err != nil {
			return nil, err
		}
	}
	log.Ctx(ctx).Info().Str("spanner-emulator-host", os.Getenv(emulatorSettingKey)).Msg("spanner emulator")
	log.Ctx(ctx).Info().Str("credentials", credentialsFilePath).Str("db", database).Msg("connecting")
	client, err := spanner.NewClient(ctx, database, option.WithCredentialsFile(credentialsFilePath))
	if err != nil {
		return nil, err
	}

	adminClient, err := admin.NewDatabaseAdminClient(ctx, option.WithCredentialsFile(credentialsFilePath))
	if err != nil {
		return nil, err
	}

	return &SpannerMigrationDriver{client, adminClient}, nil
}

func (smd *SpannerMigrationDriver) Version(ctx context.Context) (string, error) {
	var schemaRevision string
	if err := smd.client.Single().Read(
		ctx,
		tableSchemaVersion,
		spanner.AllKeys(),
		[]string{colVersionNum},
	).Do(func(r *spanner.Row) error {
		return r.Columns(&schemaRevision)
	}); err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			// There is no schema table, empty database
			return "", nil
		}
		return "", err
	}

	return schemaRevision, nil
}

// Conn returns the underlying spanner clients in a Wrapper instance for MigrationFunc to use
func (smd *SpannerMigrationDriver) Conn() Wrapper {
	return Wrapper{client: smd.client, adminClient: smd.adminClient}
}

func (smd *SpannerMigrationDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[*spanner.ReadWriteTransaction]) error {
	_, err := smd.client.ReadWriteTransaction(ctx, func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
		return f(ctx, rwt)
	})
	return err
}

func (smd *SpannerMigrationDriver) WriteVersion(_ context.Context, rwt *spanner.ReadWriteTransaction, version, replaced string) error {
	return rwt.BufferWrite([]*spanner.Mutation{
		spanner.Delete(tableSchemaVersion, spanner.KeySetFromKeys(spanner.Key{replaced})),
		spanner.Insert(tableSchemaVersion, []string{colVersionNum}, []interface{}{version}),
	})
}

func (smd *SpannerMigrationDriver) Close(_ context.Context) error {
	smd.client.Close()
	return nil
}

var _ migrate.Driver[Wrapper, *spanner.ReadWriteTransaction] = &SpannerMigrationDriver{}
