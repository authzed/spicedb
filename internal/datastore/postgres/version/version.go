package version

// MinimumSupportedPostgresVersion is the minimum version of Postgres supported for this driver.
//
// NOTE: must match a tag on DockerHub for the `postgres` image.
const MinimumSupportedPostgresVersion = "14"

// LatestTestedPostgresVersion is the latest version of Postgres that has been tested with this driver.
//
// NOTE: must match a tag on DockerHub for the `postgres` image.
const LatestTestedPostgresVersion = "18"
