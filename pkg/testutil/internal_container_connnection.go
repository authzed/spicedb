package testutil

import (
	"fmt"
	"net/url"
	"regexp"
)

// This is needed because the containers speak over their
// default ports, not over the host-mapped ports that the
// container exposes.
var engineDefaultPortMap = map[string]string{
	"cockroachdb": "26257",
	"postgres":    "5432",
	"mysql":       "3306",
	"spanner":     "9010",
}

func InternalConnString(dbConnString, driverName string) (string, error) {
	dbURL, err := url.Parse(dbConnString)
	if err != nil {
		return "", err
	}
	defaultPort, ok := engineDefaultPortMap[driverName]
	if !ok {
		return "", fmt.Errorf("missing default port for %s", driverName)
	}

	// NOTE: we need to replace this because the migrate container
	// lives on the same network as the DB container - it uses
	// the internal hostname and port.
	// We ignore the case where the host is unset because that's spanner
	// and spanner is a special child.
	if dbURL.Host != "" {
		dbURL.Host = fmt.Sprintf("%s:%s", driverName, defaultPort)
	}
	return dbURL.String(), nil
}

// InternalConnectionEnvVars takes a db connection URI and a database driver name
// and returns the environment variables necessary for a SpiceDB pod to talk to a container
// running that datastore over the internal docker network. this is necessary because otherwise
// the db URI makes the assumption that you're connecting on the host.
func InternalConnectionEnvVars(dbConnURI, driverName string) (map[string]string, error) {
	envVars := make(map[string]string)
	envVars["SPICEDB_DATASTORE_ENGINE"] = driverName
	connURI, err := InternalConnString(dbConnURI, driverName)
	if err != nil {
		return nil, err
	}
	envVars["SPICEDB_DATASTORE_CONN_URI"] = connURI

	// if the driver is spanner, we need to set the environment variable that it
	// reaches for within the container.
	if driverName == "spanner" {
		envVars["SPANNER_EMULATOR_HOST"] = "spanner:" + engineDefaultPortMap["spanner"]
	}

	// if the driver is mysql, we handle it separately, because the DSN for it
	// isn't a normal URL and has a `tcp(localhost:xxxx)` block in it. We need
	// to point that at the internal network.
	if driverName == "mysql" {
		envVars["SPICEDB_DATASTORE_CONN_URI"] = regexp.MustCompile(`localhost:\d+`).ReplaceAllString(dbConnURI, fmt.Sprintf("%s:%s", driverName, engineDefaultPortMap["mysql"]))
	}
	return envVars, nil
}
