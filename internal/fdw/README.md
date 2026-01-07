# SpiceDB Postgres Foreign Data Wrapper (FDW)

> **EXPERIMENTAL**: This feature is experimental and subject to change. The API, behavior, and configuration options may change in future releases without notice.

The SpiceDB Postgres FDW provides a Postgres-compatible interface for querying SpiceDB permissions and relationships data using standard SQL. This allows you to integrate SpiceDB with existing Postgres-based tools and applications through Foreign Data Wrapper support.

## Overview

The FDW proxy implements the Postgres wire protocol and translates SQL queries into SpiceDB API calls. It exposes virtual SQL tables that map to SpiceDB concepts:

- **`permissions`**: Query and check permissions (CheckPermission, LookupResources, LookupSubjects)
- **`relationships`**: Read, insert, and delete relationships
- **`schema`**: Read SpiceDB schema definitions

## Quick Start

### 1. Start the FDW Proxy

#### Using Docker (Recommended)

```bash
docker run --rm -p 5432:5432 \
  authzed/spicedb \
  postgres-fdw \
  --spicedb-api-endpoint localhost:50051 \
  --spicedb-access-token-secret "your-spicedb-token" \
  --postgres-endpoint ":5432" \
  --postgres-username "postgres" \
  --postgres-access-token-secret "your-fdw-password"
```

#### Using the Binary

```bash
spicedb postgres-fdw \
  --spicedb-api-endpoint localhost:50051 \
  --spicedb-access-token-secret "your-spicedb-token" \
  --postgres-endpoint ":5432" \
  --postgres-username "postgres" \
  --postgres-access-token-secret "your-fdw-password"
```

Or using environment variables:

```bash
export SPICEDB_SPICEDB_API_ENDPOINT="localhost:50051"
export SPICEDB_SPICEDB_ACCESS_TOKEN_SECRET="your-spicedb-token"
export SPICEDB_POSTGRES_ENDPOINT=":5432"
export SPICEDB_POSTGRES_USERNAME="postgres"
export SPICEDB_POSTGRES_ACCESS_TOKEN_SECRET="your-fdw-password"

spicedb postgres-fdw
```

### 2. Configure Postgres Foreign Data Wrapper

In your Postgres database, install and configure the FDW extension. See [`configuration.sql`](../../configuration.sql) for a complete setup example:

```sql
-- Install the postgres_fdw extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create a foreign server pointing to the FDW proxy
CREATE SERVER spicedb_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'localhost',
    port '5432',
    dbname 'ignored'
  );

-- Create user mapping with authentication credentials
CREATE USER MAPPING FOR CURRENT_USER
  SERVER spicedb_server
  OPTIONS (
    user 'postgres',
    password 'your-fdw-password'
  );

-- Import foreign tables
IMPORT FOREIGN SCHEMA public
  LIMIT TO (permissions, relationships, schema)
  FROM SERVER spicedb_server
  INTO public;
```

### 3. Query SpiceDB Data

Once configured, you can query SpiceDB using standard SQL:

#### Check Permissions

```sql
-- Check if user:alice has permission to view document:readme
SELECT has_permission
FROM permissions
WHERE resource_type = 'document'
  AND resource_id = 'readme'
  AND permission = 'view'
  AND subject_type = 'user'
  AND subject_id = 'alice';
```

#### Lookup Resources

```sql
-- Find all documents that user:alice can view
SELECT resource_id
FROM permissions
WHERE resource_type = 'document'
  AND permission = 'view'
  AND subject_type = 'user'
  AND subject_id = 'alice'
  AND has_permission = true;
```

#### Lookup Subjects

```sql
-- Find all users who can view document:readme
SELECT subject_id
FROM permissions
WHERE resource_type = 'document'
  AND resource_id = 'readme'
  AND permission = 'view'
  AND subject_type = 'user'
  AND has_permission = true;
```

#### Query Relationships

```sql
-- Read relationships for a specific resource
SELECT resource_type, resource_id, relation, subject_type, subject_id
FROM relationships
WHERE resource_type = 'document'
  AND resource_id = 'readme';
```

#### Insert Relationships

```sql
-- Add a new relationship
INSERT INTO relationships (resource_type, resource_id, relation, subject_type, subject_id)
VALUES ('document', 'readme', 'viewer', 'user', 'alice');
```

#### Delete Relationships

```sql
-- Remove a relationship
DELETE FROM relationships
WHERE resource_type = 'document'
  AND resource_id = 'readme'
  AND relation = 'viewer'
  AND subject_type = 'user'
  AND subject_id = 'alice';
```

#### Read Schema

```sql
-- Get all schema definitions
SELECT definition FROM schema;
```

## Configuration Options

### Command Line Flags

**SpiceDB Connection:**

- `--spicedb-api-endpoint`: SpiceDB API endpoint (default: `localhost:50051`)
- `--spicedb-access-token-secret`: **(required)** SpiceDB API access token
- `--spicedb-insecure`: Use insecure connection to SpiceDB

**FDW Server:**

- `--postgres-endpoint`: FDW server listen address (default: `:5432`)
- `--postgres-username`: Username for Postgres authentication (default: `postgres`)
- `--postgres-access-token-secret`: **(required)** Password for Postgres authentication
- `--shutdown-grace-period`: Graceful shutdown timeout (default: `0s`)

### Environment Variables

All flags can be set via environment variables with the `SPICEDB_` prefix:

```bash
SPICEDB_SPICEDB_API_ENDPOINT
SPICEDB_SPICEDB_ACCESS_TOKEN_SECRET
SPICEDB_SPICEDB_INSECURE
SPICEDB_POSTGRES_ENDPOINT
SPICEDB_POSTGRES_USERNAME
SPICEDB_POSTGRES_ACCESS_TOKEN_SECRET
SPICEDB_SHUTDOWN_GRACE_PERIOD
```

## Table Schemas

### `permissions` Table

| Column | Type | Description |
|--------|------|-------------|
| `resource_type` | text | Resource type (e.g., 'document') |
| `resource_id` | text | Resource ID |
| `permission` | text | Permission name |
| `subject_type` | text | Subject type (e.g., 'user') |
| `subject_id` | text | Subject ID |
| `optional_subject_relation` | text | Optional subject relation |
| `has_permission` | boolean | Whether permission is granted |
| `consistency` | text | Consistency token (ZedToken) |

**Supported Operations:**

- SELECT (CheckPermission, LookupResources, LookupSubjects)

### `relationships` Table

| Column | Type | Description |
|--------|------|-------------|
| `resource_type` | text | Resource type |
| `resource_id` | text | Resource ID |
| `relation` | text | Relation name |
| `subject_type` | text | Subject type |
| `subject_id` | text | Subject ID |
| `optional_subject_relation` | text | Optional subject relation |
| `optional_caveat_name` | text | Optional caveat name |
| `optional_caveat_context` | jsonb | Optional caveat context |
| `consistency` | text | Consistency token (ZedToken) |

**Supported Operations:**

- SELECT (ReadRelationships)
- INSERT (WriteRelationships)
- DELETE (DeleteRelationships)

### `schema` Table

| Column | Type | Description |
|--------|------|-------------|
| `definition` | text | Schema definition in Zed format |

**Supported Operations:**

- SELECT (ReadSchema)

## Consistency and Transactions

The FDW supports consistency control through the `consistency` column:

- **`minimize_latency`**: Default, uses the newest available snapshot
- **`fully_consistent`**: Waits for a fully consistent view
- **`<zedtoken>`**: Uses a specific consistency token
- **`@<zedtoken>`**: Uses exact snapshot matching

Example:

```sql
-- Get a consistent view
SELECT resource_id, consistency
FROM permissions
WHERE resource_type = 'document'
  AND permission = 'view'
  AND subject_type = 'user'
  AND subject_id = 'alice'
  AND consistency = 'fully_consistent';
```

## Cursors

The FDW supports Postgres cursors for large result sets:

```sql
BEGIN;

DECLARE my_cursor CURSOR FOR
  SELECT resource_id FROM permissions
  WHERE resource_type = 'document'
    AND permission = 'view'
    AND subject_type = 'user'
    AND subject_id = 'alice';

FETCH 100 FROM my_cursor;
FETCH 100 FROM my_cursor;

CLOSE my_cursor;
COMMIT;
```

## Limitations

- **Joins**: Cross-table joins are not supported
- **Aggregations**: SUM, COUNT, etc. are not supported (performed client-side by Postgres)
- **Ordering**: ORDER BY clauses are not supported by the FDW itself
- **Subqueries**: Not supported
- **Complex WHERE clauses**: Only simple equality predicates and AND conditions are supported

## Performance Considerations

1. **Query Planning**: The FDW collects statistics to help Postgres make better query planning decisions
2. **Cursors**: Use cursors for large result sets to avoid loading everything into memory
3. **Consistency**: Use `minimize_latency` for best performance when strong consistency isn't required
4. **Filtering**: Push down as many WHERE conditions as possible to reduce data transfer

## Architecture

The FDW implementation consists of:

- **`fdw`**: Main package with Postgres wire protocol server (`PgBackend`)
- **`fdw/tables`**: SQL table handlers (permissions, relationships, schema)
- **`fdw/common`**: Error handling utilities
- **`fdw/explain`**: EXPLAIN query plan generation
- **`fdw/stats`**: Query statistics collection

## Examples

For complete configuration examples and SQL setup scripts, see:

- [`configuration.sql`](./configuration.sql) - Full Postgres FDW setup

### Query Errors

Enable debug logging:

```bash
spicedb postgres-fdw --log-level debug ...
```

Common issues:

- **"table does not exist"**: Table name must be `permissions`, `relationships`, or `schema`
- **"feature not supported"**: Query uses unsupported SQL features (see Limitations)
- **"invalid username"**: Check `--postgres-username` matches user mapping in Postgres
