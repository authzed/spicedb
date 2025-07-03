package version

// MinimumSupportedCockroachDBVersion is the minimum version of CockroachDB supported for this driver.
//
// NOTE: must match a tag on DockerHub for the `cockroachdb/cockroach` image, without the `v` prefix.
const MinimumSupportedCockroachDBVersion = "23.1.30"

// LatestTestedCockroachDBVersion is the latest version of CockroachDB that has been tested with this driver.
//
// NOTE: must match a tag on DockerHub for the `cockroachdb/cockroach` image, without the `v` prefix.
const LatestTestedCockroachDBVersion = "24.2.9"
