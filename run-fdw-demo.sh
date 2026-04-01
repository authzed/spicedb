#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────
NETWORK="spicedb-fdw-demo"
SPICEDB_IMAGE="spicedb-fdw-demo:local"
POSTGRES_IMAGE="postgres:16"

SPICEDB_CONTAINER="spicedb-demo"
FDW_CONTAINER="spicedb-fdw-demo"
POSTGRES_CONTAINER="postgres-fdw-demo"

SPICEDB_TOKEN="demotokensecret"
SPICEDB_GRPC_PORT=50051

FDW_PORT=15432
FDW_USER="fdwuser"
FDW_PASSWORD="fdwpassword"

PG_PORT=25432
PG_USER="postgres"
PG_PASSWORD="pgpassword"
PG_DB="fdw_demo"

# ── Cleanup ────────────────────────────────────────────────────
cleanup() {
  echo "Cleaning up containers and network..."
  docker rm -f "$SPICEDB_CONTAINER" "$FDW_CONTAINER" "$POSTGRES_CONTAINER" 2>/dev/null || true
  docker network rm "$NETWORK" 2>/dev/null || true
}

trap cleanup EXIT

# Remove any leftover containers from a previous run
docker rm -f "$SPICEDB_CONTAINER" "$FDW_CONTAINER" "$POSTGRES_CONTAINER" 2>/dev/null || true
docker network rm "$NETWORK" 2>/dev/null || true

# ── Build local SpiceDB image ──────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Building SpiceDB from local source..."
docker build -t "$SPICEDB_IMAGE" "$SCRIPT_DIR"

# ── Network ────────────────────────────────────────────────────
echo "Creating Docker network..."
docker network create "$NETWORK"

# ── SpiceDB ────────────────────────────────────────────────────
echo "Starting SpiceDB..."
docker run -d --name "$SPICEDB_CONTAINER" \
  --network "$NETWORK" \
  "$SPICEDB_IMAGE" \
  serve \
  --grpc-preshared-key "$SPICEDB_TOKEN" \
  --datastore-engine memory

# Wait for SpiceDB to be ready by checking logs for gRPC server readiness
echo "Waiting for SpiceDB to start..."
for i in $(seq 1 30); do
  if docker logs "$SPICEDB_CONTAINER" 2>&1 | grep -q "grpc server started serving"; then
    break
  fi
  sleep 1
done

# Write a demo schema and relationships into SpiceDB using the zed CLI
ZED_IMAGE="authzed/zed:latest"

echo "Writing demo schema..."
SCHEMA_FILE="$(mktemp)"
cat > "$SCHEMA_FILE" <<'SCHEMA'
definition user {}

definition document {
    relation owner: user
    relation viewer: user
    permission view = viewer + owner
    permission edit = owner
}
SCHEMA

docker run --rm --network "$NETWORK" \
  -e ZED_ENDPOINT="$SPICEDB_CONTAINER:$SPICEDB_GRPC_PORT" \
  -e ZED_TOKEN="$SPICEDB_TOKEN" \
  -e ZED_INSECURE=true \
  -v "$SCHEMA_FILE:/tmp/schema.zed:ro" \
  "$ZED_IMAGE" \
  schema write /tmp/schema.zed

rm -f "$SCHEMA_FILE"

echo "Writing demo relationships..."
# Format: resource:id relation subject:id
while IFS=' ' read -r resource relation subject; do
  docker run --rm --network "$NETWORK" \
    -e ZED_ENDPOINT="$SPICEDB_CONTAINER:$SPICEDB_GRPC_PORT" \
    -e ZED_TOKEN="$SPICEDB_TOKEN" \
    -e ZED_INSECURE=true \
    "$ZED_IMAGE" \
    relationship create "$resource" "$relation" "$subject"
done <<'RELS'
document:readme owner user:alice
document:readme viewer user:bob
document:design owner user:bob
document:design viewer user:alice
document:secret owner user:charlie
document:roadmap owner user:charlie
document:roadmap viewer user:alice
RELS

echo "SpiceDB is ready with demo data."

# ── FDW Proxy ──────────────────────────────────────────────────
echo "Starting SpiceDB FDW proxy..."
docker run -d --name "$FDW_CONTAINER" \
  --network "$NETWORK" \
  "$SPICEDB_IMAGE" \
  postgres-fdw \
  --spicedb-api-endpoint "$SPICEDB_CONTAINER:$SPICEDB_GRPC_PORT" \
  --spicedb-access-token-secret "$SPICEDB_TOKEN" \
  --spicedb-insecure \
  --postgres-endpoint ":5432" \
  --postgres-username "$FDW_USER" \
  --postgres-access-token-secret "$FDW_PASSWORD"

echo "FDW proxy is ready."

# ── Postgres ───────────────────────────────────────────────────
echo "Starting Postgres..."
docker run -d --name "$POSTGRES_CONTAINER" \
  --network "$NETWORK" \
  -p "$PG_PORT:5432" \
  -e POSTGRES_USER="$PG_USER" \
  -e POSTGRES_PASSWORD="$PG_PASSWORD" \
  -e POSTGRES_DB="$PG_DB" \
  "$POSTGRES_IMAGE"

# Wait for Postgres to accept connections
echo "Waiting for Postgres to start..."
for i in $(seq 1 30); do
  if docker exec "$POSTGRES_CONTAINER" pg_isready -U "$PG_USER" -d "$PG_DB" 2>/dev/null | grep -q "accepting"; then
    break
  fi
  sleep 1
done
sleep 1

# ── Configure FDW in Postgres ─────────────────────────────────
echo "Configuring FDW extension in Postgres..."
docker exec "$POSTGRES_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER spicedb_fdw
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '$FDW_CONTAINER', dbname 'default', port '5432', use_remote_estimate 'true');

CREATE USER MAPPING FOR $PG_USER
  SERVER spicedb_fdw
  OPTIONS (user '$FDW_USER', password '$FDW_PASSWORD');

CREATE FOREIGN TABLE relationships (
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    relation text NOT NULL,
    subject_type text NOT NULL,
    subject_id text NOT NULL,
    optional_subject_relation text default '',
    caveat_name text default '',
    caveat_context text default '',
    consistency text default ''
) SERVER spicedb_fdw;

CREATE FOREIGN TABLE permissions (
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    permission text NOT NULL,
    subject_type text NOT NULL,
    subject_id text NOT NULL,
    optional_subject_relation text NOT NULL,
    has_permission boolean,
    consistency text default ''
) SERVER spicedb_fdw;

CREATE FOREIGN TABLE schema (schema_text text) SERVER spicedb_fdw;

-- Local application table with document metadata
CREATE TABLE documents (
    id text PRIMARY KEY,
    title text NOT NULL,
    created_by text NOT NULL,
    created_at timestamp DEFAULT now()
);

INSERT INTO documents (id, title, created_by) VALUES
    ('readme', 'Project README', 'alice'),
    ('design', 'Design Spec', 'bob'),
    ('secret', 'Secret Plans', 'charlie'),
    ('roadmap', 'Product Roadmap', 'charlie');
"

echo ""
echo "============================================"
echo "  SpiceDB FDW Demo is ready!"
echo "============================================"
echo ""
echo "Connect with:"
echo ""
echo "  psql -h localhost -p $PG_PORT -U $PG_USER -d $PG_DB"
echo "  (password: $PG_PASSWORD)"
echo ""
echo "Try these queries:"
echo ""
echo "  -- Read the schema"
echo "  SELECT * FROM schema;"
echo ""
echo "  -- List all relationships"
echo "  SELECT * FROM relationships WHERE resource_type = 'document';"
echo ""
echo "  -- Check if alice can view document:readme"
echo "  SELECT has_permission FROM permissions"
echo "    WHERE resource_type = 'document' AND resource_id = 'readme'"
echo "    AND permission = 'view' AND subject_type = 'user' AND subject_id = 'alice';"
echo ""
echo "  -- Find all documents alice can view"
echo "  SELECT resource_id FROM permissions"
echo "    WHERE resource_type = 'document' AND permission = 'view'"
echo "    AND subject_type = 'user' AND subject_id = 'alice' AND has_permission = true;"
echo ""
echo "  -- Lookup resources joined with the documents table:"
echo "  -- Find all documents bob can view, with their metadata"
echo "  SELECT d.id, d.title, d.created_by, d.created_at"
echo "    FROM documents d"
echo "    JOIN permissions p ON d.id = p.resource_id"
echo "    WHERE p.resource_type = 'document' AND p.permission = 'view'"
echo "    AND p.subject_type = 'user' AND p.subject_id = 'bob';"
echo ""
echo "  -- Grant bob viewer access to the roadmap document"
echo "  INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id)"
echo "    VALUES ('document', 'roadmap', 'viewer', 'user', 'bob');"
echo ""
echo "Press Ctrl+C to stop and clean up all containers."
echo ""

# Keep running until interrupted
echo "Following SpiceDB FDW proxy logs (Ctrl+C to stop)..."
docker logs -f "$FDW_CONTAINER"
