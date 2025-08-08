package rowsclose

import (
	"database/sql"
	"fmt"
)

func GoodRowsHandling() error {
	db, _ := sql.Open("postgres", "connection-string")
	rows, err := db.Query("SELECT * FROM table")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// Process row
	}
	
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}

func BadRowsHandling() error {
	db, _ := sql.Open("postgres", "connection-string")
	rows, err := db.Query("SELECT * FROM table")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// Process row
	}
	
	rows.Close() // want "rows.Close\\(\\) call should be immediately followed by 'if rows.Err\\(\\) != nil' check"
	
	fmt.Println("some other operation")
	
	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}

func MissingErrorCheck() error {
	db, _ := sql.Open("postgres", "connection-string")
	rows, err := db.Query("SELECT * FROM table")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// Process row
	}
	
	rows.Close() // want "rows.Close\\(\\) call should be immediately followed by 'if rows.Err\\(\\) != nil' check"
	
	return nil
}

func DeferredCloseIsOK() error {
	db, _ := sql.Open("postgres", "connection-string")
	rows, err := db.Query("SELECT * FROM table")
	if err != nil {
		return err
	}
	defer rows.Close() // This should not trigger the linter

	for rows.Next() {
		// Process row
	}
	
	return rows.Err()
}

func MultipleRowsVariables() error {
	db, _ := sql.Open("postgres", "connection-string")
	
	userRows, err := db.Query("SELECT * FROM users")
	if err != nil {
		return err
	}
	defer userRows.Close()

	for userRows.Next() {
		// Process row
	}
	
	userRows.Close()
	if userRows.Err() != nil {
		return userRows.Err()
	}

	orderRows, err := db.Query("SELECT * FROM orders")
	if err != nil {
		return err
	}
	defer orderRows.Close()

	for orderRows.Next() {
		// Process row
	}
	
	orderRows.Close() // want "rows.Close\\(\\) call should be immediately followed by 'if rows.Err\\(\\) != nil' check"
	
	fmt.Println("processing complete")
	
	if orderRows.Err() != nil {
		return orderRows.Err()
	}

	return nil
}

func SimpleRowsVariable() error {
	db, _ := sql.Open("postgres", "connection-string")
	rows, err := db.Query("SELECT * FROM simple")
	if err != nil {
		return err
	}
	defer rows.Close()

	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}