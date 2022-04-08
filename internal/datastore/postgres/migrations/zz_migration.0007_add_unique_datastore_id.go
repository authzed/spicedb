package migrations

import "github.com/google/uuid"

const createUniqueIDTable = `CREATE TABLE metadata (
	unique_id VARCHAR PRIMARY KEY
);`

const insertUniqueID = `INSERT INTO metadata (unique_id) VALUES ($1);`

func init() {
	if err := DatabaseMigrations.Register("add-unique-datastore-id", "add-gc-index", func(apd *AlembicPostgresDriver) error {
		tx, err := apd.db.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = tx.Exec(createUniqueIDTable)
		if err != nil {
			return err
		}

		_, err = tx.Exec(insertUniqueID, uuid.NewString())
		if err != nil {
			return err
		}

		return tx.Commit()
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
