// +build go1.10

package sqlmw

import "database/sql/driver"

var _ driver.DriverContext = wrappedDriver{}

func (d wrappedDriver) OpenConnector(name string) (driver.Connector, error) {
	driver, ok := d.parent.(driver.DriverContext)
	if !ok {
		return wrappedConnector{
			parent:    dsnConnector{dsn: name, driver: d.parent},
			driverRef: &d,
		}, nil
	}
	conn, err := driver.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return wrappedConnector{parent: conn, driverRef: &d}, nil
}
