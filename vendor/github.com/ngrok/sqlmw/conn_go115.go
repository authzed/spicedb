// +build go1.15

package sqlmw

import (
	"database/sql/driver"
)

var _ driver.SessionResetter = wrappedConn{}

func (c wrappedConn) IsValid() bool {
	conn, ok := c.parent.(driver.Validator)
	if !ok {
		// the default if driver.Validator is not supported
		return true
	}

	return conn.IsValid()
}
