package migrations

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	createUniqueIDTable = `CREATE TABLE metadata (
		unique_id VARCHAR PRIMARY KEY
	);
`
	insertUniqueID = `INSERT INTO metadata (unique_id) VALUES ('%s');
`
)

func init() {
	m := &PostgresMigration{
		version:         "add-unique-datastore-id",
		replaces:        "add-gc-index",
		expected:        "add-gc-index",
		migrationType:   DDL,
		migrationSafety: expand,
	}
	m.Begin()
	m.Statement(createUniqueIDTable)
	m.Statement(fmt.Sprintf(insertUniqueID, uuid.New()))
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)
}
