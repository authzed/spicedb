#!/bin/bash
set -e

PGDATA="/var/lib/postgresql/data"

# Wait for primary to be ready
until pg_isready -h postgres-primary -U spicedb 2>/dev/null; do
  echo "Waiting for primary to be ready..."
  sleep 2
done

# Only run base backup if data directory is empty
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "Setting up replica from primary..."

  # Create base backup from primary
  PGPASSWORD=replicator pg_basebackup \
    -h postgres-primary \
    -D "$PGDATA" \
    -U replicator \
    -v \
    -P \
    -X stream \
    -R

  # Set proper permissions
  chmod 700 "$PGDATA"
  chown -R postgres:postgres "$PGDATA"

  echo "Replica setup complete"
else
  echo "Data directory already initialized, skipping base backup"
fi

# Start PostgreSQL as replica
exec docker-entrypoint.sh postgres -c recovery_min_apply_delay=${REPLICA_APPLY_DELAY:-0}
