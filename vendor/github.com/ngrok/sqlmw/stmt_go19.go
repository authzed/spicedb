package sqlmw

import "database/sql/driver"

var _ driver.NamedValueChecker = wrappedStmt{}

func (s wrappedStmt) CheckNamedValue(v *driver.NamedValue) error {
	if checker, ok := s.parent.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(v)
	}

	if checker, ok := s.conn.parent.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(v)
	}

	return defaultCheckNamedValue(v)
}
