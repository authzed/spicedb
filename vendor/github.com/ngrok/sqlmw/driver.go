package sqlmw

import "database/sql/driver"

// driver wraps a sql.Driver with an interceptor.
type wrappedDriver struct {
	intr   Interceptor
	parent driver.Driver
}

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Driver = wrappedDriver{}
)

// WrapDriver will wrap the passed SQL driver and return a new sql driver that uses it and also logs and traces calls using the passed logger and tracer
// The returned driver will still have to be registered with the sql package before it can be used.
//

// Driver returns the supplied driver.Driver with a new object that has all of its calls intercepted by the supplied
// Interceptor object.
//
// Important note: Seeing as the context passed into the various instrumentation calls this package calls,
// Any call without a context passed will not be intercepted. Please be sure to use the ___Context() and BeginTx()
// function calls added in Go 1.8 instead of the older calls which do not accept a context.
func Driver(driver driver.Driver, intr Interceptor) driver.Driver {
	return wrappedDriver{parent: driver, intr: intr}
}

// Open implements the database/sql/driver.Driver interface for WrappedDriver.
func (d wrappedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}

	return wrappedConn{intr: d.intr, parent: conn}, nil
}
