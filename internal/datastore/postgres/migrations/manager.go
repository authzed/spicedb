package migrations

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/authzed/spicedb/pkg/migrate"

	"github.com/jackc/pgx/v4"
)

// DatabaseMigrations implements a migration manager for the Postgres Driver.
var DatabaseMigrations = migrate.NewManager[*AlembicPostgresDriver, *pgx.Conn, pgx.Tx]()

type PGMigrationSet []*PostgresMigration

var PostgresMigrations PGMigrationSet = make([]*PostgresMigration, 0)

func (p PGMigrationSet) Sort() {
	slices.SortFunc(p, func(a *PostgresMigration, b *PostgresMigration) bool {
		return b.replaces == a.version
	})
}

func RegisterPGMigration(p *PostgresMigration) {
	if p.migrationType == "" {
		panic("migration missing type")
	}
	if p.migrationSafety == "" {
		panic("migration missing safety")
	}
	PostgresMigrations = append(PostgresMigrations, p)
	if err := DatabaseMigrations.Register(p.version, p.replaces, p.expected, p.Run, nil); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

type migrationType string

var (
	DDL migrationType = "DDL"
	DML migrationType = "DML"
)

type migrationSafety string

var (
	expand   migrationSafety = "expand"
	contract migrationSafety = "contract"
)

type PostgresMigration struct {
	version         string
	replaces        string
	expected        string
	command         strings.Builder
	suffix          string
	migrationType   migrationType
	migrationSafety migrationSafety
}

func (p *PostgresMigration) Run(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, p.command.String())
	return err
}

func (p *PostgresMigration) Begin() {
	p.command.WriteString("BEGIN;\n")
}

func (p *PostgresMigration) Commit() {
	p.command.WriteString("COMMIT;")
}

func (p *PostgresMigration) WriteVersion() {
	if p.version == "" {
		panic("cannot write empty version")
	}
	p.command.WriteString(fmt.Sprintf("UPDATE alembic_version SET version_num='%[1]s' WHERE version_num='%[2]s';\n", p.version, p.replaces))
}

func (p *PostgresMigration) WriteVersionOverride(previous string) {
	if p.version == "" {
		panic("cannot write empty version")
	}
	p.command.WriteString(fmt.Sprintf("UPDATE alembic_version SET version_num='%[1]s' WHERE version_num='%[2]s';\n", p.version, previous))
}

func (p *PostgresMigration) Statement(stmt string) {
	p.command.WriteString(stmt)
}

func (p *PostgresMigration) FileName(prefix string) string {
	suffix := p.suffix
	if suffix == "" {
		suffix = ".sql"
	}
	return strings.Join([]string{
		prefix,
		string(p.migrationType),
		string(p.migrationSafety),
		strings.ReplaceAll(p.version, "-", "_"),
	}, "_") + suffix
}

func (p *PostgresMigration) Bytes() []byte {
	return []byte(p.command.String())
}
