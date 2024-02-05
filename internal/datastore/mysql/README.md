# MySQL Datastore

**Minimum required version** can be found here defined as `MinimumSupportedMySQLVersion` in [version.go](version/version.go)

This datastore implementation allows you to use a MySQL database as the backing durable storage for SpiceDB.

## Usage Caveats

The MySQL datastore only supports a single primary node, and does not support read replicas. It is therefore only recommended for installations that can scale the MySQL instance vertically and cannot be used efficiently in a mulit-region installation.
