# SpiceDB Postgres FDW Proxy Architecture

## Overview

The SpiceDB Postgres FDW (Foreign Data Wrapper) proxy is a translation layer that implements the PostgreSQL wire protocol and converts SQL queries into SpiceDB API calls. This allows standard PostgreSQL clients and tools to interact with SpiceDB's permissions and relationships data as if they were querying regular PostgreSQL tables.
It also allows existing Postgres *databases* to access SpiceDB data, so long as they have the Postgres Foreign Data Wrapper [postgres_fdw](https://www.postgresql.org/docs/current/postgres-fdw.html) support enabled.

```
┌─────────────────┐
│  PostgreSQL     │
│  Client/Tool    │
│  (psql, app)    │
└────────┬────────┘
         │ SQL Query
         ▼
┌─────────────────────────────────────┐
│      PostgreSQL Database            │
│  ┌───────────────────────────────┐  │
│  │   postgres_fdw Extension      │  │
│  │  (Foreign Data Wrapper)       │  │
│  └───────────┬───────────────────┘  │
│              │                      │
│  ┌───────────▼───────────────────┐  │
│  │   Foreign Server Config       │  │
│  │   host: fdw-proxy-host        │  │
│  │   port: 5432                  │  │
│  └───────────┬───────────────────┘  │
│              │                      │
│  ┌───────────▼───────────────────┐  │
│  │   Foreign Tables              │  │
│  │   • permissions               │  │
│  │   • relationships             │  │
│  │   • schema                    │  │
│  └───────────┬───────────────────┘  │
└──────────────┼──────────────────────┘
               │ Postgres Wire Protocol
               ▼
┌─────────────────────────────────────┐
│      FDW Proxy (PgBackend)          │
│  ┌───────────────────────────────┐  │
│  │   Wire Protocol Handler       │  │
│  │  (Authentication, Parsing)    │  │
│  └───────────┬───────────────────┘  │
│              │                      │
│  ┌───────────▼───────────────────┐  │
│  │   Query Router                │  │
│  │  (SELECT, INSERT, DELETE,     │  │
│  │   EXPLAIN, CURSOR ops)        │  │
│  └───────────┬───────────────────┘  │
│              │                      │
│  ┌───────────▼───────────────────┐  │
│  │   Table Handlers              │  │
│  │  • permissions                │  │
│  │  • relationships              │  │
│  │  • schema                     │  │
│  └───────────┬───────────────────┘  │
└──────────────┼──────────────────────┘
               │ SpiceDB gRPC API Calls
               ▼
┌─────────────────────────────────────┐
│         SpiceDB Server              │
│  • CheckPermission                  │
│  • LookupResources                  │
│  • LookupSubjects                   │
│  • ReadRelationships                │
│  • WriteRelationships               │
│  • DeleteRelationships              │
│  • ReadSchema                       │
└─────────────────────────────────────┘
```

## Core Components

### 1. PgBackend - Wire Protocol Server

**Location**: `pgserver.go`

The `PgBackend` struct is the main entry point that implements the PostgreSQL wire protocol server:

```go
type PgBackend struct {
    client   *authzed.Client  // SpiceDB API client
    server   *wire.Server     // Postgres wire protocol server
    username string            // Authentication username
    password string            // Authentication password
}
```

**Responsibilities**:

- Listen on a TCP port for PostgreSQL protocol connections
- Authenticate incoming connections using cleartext password authentication
- Parse incoming SQL queries using the `pg_query` parser
- Route queries to appropriate handlers
- Manage the lifecycle of the server

**Flow**:
```
Client Connect
     │
     ▼
┌────────────────┐
│ validateAuth() │ ───> Check username/password
└────────┬───────┘
         │ Success
         ▼
┌────────────────┐
│ handler()      │ ───> Parse & route SQL query
└────────────────┘
```

### 2. Session Management

**Location**: `session.go`

Each PostgreSQL connection maintains a session that tracks:

- Transaction state (in/out of transaction)
- Active cursors (name → cursor mapping)

```
┌─────────────────────────────────────┐
│           Session                   │
│  ┌───────────────────────────────┐  │
│  │  withinTransaction: bool      │  │
│  └───────────────────────────────┘  │
│                                     │
│  ┌───────────────────────────────┐  │
│  │  cursors: map[string]*cursor  │  │
│  │    "cursor1" → cursor{...}    │  │
│  │    "cursor2" → cursor{...}    │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

**Middleware**: The `sessionMiddleware` function initializes a new session for each connection and stores it in the request context.

### 3. Transaction Handling

**Location**: `transaction.go`

The FDW proxy implements basic transaction semantics for PostgreSQL compatibility:

```
BEGIN
  │
  ├─> Set withinTransaction = true
  │
  ├─> [Execute queries...]
  │
  └─> COMMIT/ROLLBACK
        │
        └─> Set withinTransaction = false
```

**Important Notes**:

- Transactions are tracked for cursor lifetime management
- SpiceDB operations are still atomic per-API-call but are *not* transactionally executed themselves
- Transaction state primarily controls cursor lifecycle

### 4. Query Routing

**Location**: `pgserver.go` (`handler()` method)

The query router examines the parsed SQL AST and dispatches to specialized handlers:

```
    SQL Query
        │
        ▼
   ┌─────────┐
   │  Parse  │
   └────┬────┘
        │
        ▼
   ┌─────────────────────────────────┐
   │    Statement Type Switch        │
   └─────┬───────────────────────────┘
         │
    ┌────┴────┬────────┬─────────┬──────────┬────────────┐
    │         │        │         │          │            │
    ▼         ▼        ▼         ▼          ▼            ▼
  SELECT   INSERT   DELETE   EXPLAIN   CURSOR ops    SET/TXNS
    │         │        │         │          │            │
    ▼         ▼        ▼         ▼          ▼            ▼
 [Table   [Table   [Table   [Explain  [Cursor      [Metadata
Handler]  Handler] Handler]  Handler]  Handler]     Handler]
```

**Supported Statement Types**:

- `SET variable = value` - Ignored (for compatibility)
- `SELECT` - Query data from virtual tables
- `INSERT` - Write relationships
- `DELETE` - Remove relationships
- `EXPLAIN` - Generate query plans with cost estimates
- `DECLARE CURSOR` - Create named cursor for result streaming
- `FETCH` - Retrieve rows from cursor
- `CLOSE` - Close cursor
- `DEALLOCATE` - Remove cursor or prepared statement
- `START TRANSACTION / COMMIT / ROLLBACK` - Transaction control

### 5. Table Handlers

**Location**: `tables/` directory

Each table (permissions, relationships, schema) has its own handler that:

1. Validates the query structure
2. Extracts WHERE clause predicates
3. Determines which SpiceDB API to call
4. Executes the API call(s)
5. Formats results as PostgreSQL rows

#### Permissions Table Handler

**Location**: `tables/permissions.go`

The permissions table supports three distinct query patterns, each mapping to a different SpiceDB API:

```
┌─────────────────────────────────────────────────────────────┐
│              Permissions Table Query Router                 │
└──────────────────┬──────────────────────────────────────────┘
                   │
      ┌────────────┼────────────┐
      │            │            │
      ▼            ▼            ▼
┌───────────┐ ┌──────────────┐ ┌──────────────┐
│ALL fields │ │No resource_id│ │No subject_id │
│ provided  │ │   provided   │ │   provided   │
└─────┬─────┘ └──────┬───────┘ └──────┬───────┘
      │              │                │
      ▼              ▼                ▼
┌───────────┐ ┌──────────────┐ ┌──────────────┐
│   Check   │ │   Lookup     │ │   Lookup     │
│Permission │ │  Resources   │ │  Subjects    │
└───────────┘ └──────────────┘ └──────────────┘
```

**Query Pattern Detection**:

1. **CheckPermission** - All fields specified:
   ```sql
   SELECT has_permission FROM permissions
   WHERE resource_type = 'doc' AND resource_id = '1'
     AND permission = 'view'
     AND subject_type = 'user' AND subject_id = 'alice';
   ```
   → Calls `CheckPermission(doc:1#view@user:alice)`

2. **LookupResources** - Missing `resource_id`:
   ```sql
   SELECT resource_id FROM permissions
   WHERE resource_type = 'doc' AND permission = 'view'
     AND subject_type = 'user' AND subject_id = 'alice'
     AND has_permission = true;
   ```
   → Calls `LookupResources(user:alice, view, doc)`

3. **LookupSubjects** - Missing `subject_id`:
   ```sql
   SELECT subject_id FROM permissions
   WHERE resource_type = 'doc' AND resource_id = '1'
     AND permission = 'view'
     AND subject_type = 'user'
     AND has_permission = true;
   ```
   → Calls `LookupSubjects(doc:1, view, user)`

#### Relationships Table Handler

**Location**: `tables/relationships.go`

Supports CRUD operations on SpiceDB relationships:

```
┌────────────────────────────────────┐
│    Relationships Table Handler     │
└──────────────┬─────────────────────┘
               │
    ┌──────────┼──────────┐
    │          │          │
    ▼          ▼          ▼
┌────────┐ ┌────────┐ ┌────────┐
│ SELECT │ │ INSERT │ │ DELETE │
└───┬────┘ └───┬────┘ └───┬────┘
    │          │          │
    ▼          ▼          ▼
┌─────────┐ ┌──────────┐ ┌──────────┐
│  Read   │ │  Write   │ │  Delete  │
│Relations│ │Relations │ │Relations │
└─────────┘ └──────────┘ └──────────┘
```

**Operations**:

- **SELECT**: Streams relationships matching WHERE clause filters
- **INSERT**: Creates new relationships with optional caveats
- **DELETE**: Removes relationships matching WHERE clause

#### Schema Table Handler

**Location**: `tables/schema.go`

Provides read-only access to SpiceDB schema definitions:

```sql
SELECT definition FROM schema;
```
→ Calls `ReadSchema()` and returns Zed format definitions

### 6. Cursor Support

**Location**: `cursor.go`

Cursors enable streaming large result sets without loading everything into memory:

```
┌──────────────────────────────────────────────────────────┐
│                    Cursor Lifecycle                      │
└──────────────────────────────────────────────────────────┘

    DECLARE my_cursor CURSOR FOR SELECT ...
           │
           ▼
    ┌──────────────────┐
    │  Parse SELECT    │
    │  statement       │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │  Store in        │
    │  session.cursors │
    │  map             │
    └────────┬─────────┘
             │
             │
    ┌────────▼─────────┐
    │  FETCH 100       │
    │  FROM my_cursor  │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │  Execute query   │
    │  Get next batch  │
    │  Store position  │
    └────────┬─────────┘
             │
             │  (Can repeat FETCH)
             │
             ▼
    ┌──────────────────┐
    │  CLOSE my_cursor │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │  Remove from     │
    │  session.cursors │
    └──────────────────┘
```

**Cursor Structure**:
```go
type cursor struct {
    name       string              // Cursor name
    selectStmt *SelectStatement    // Parsed query
    parameters []Parameter         // Bound parameters
    rowsCursor RowsCursor          // Streaming state (table-specific)
}
```

**Key Features**:

- Cursors are scoped to a session
- Support forward-only iteration
- Maintain streaming state between FETCH operations
- Automatically cleaned up on DEALLOCATE or session end

### 7. Query Parsing & Pattern Matching

**Location**: `tables/select.go`

The SELECT statement parser extracts structured information from SQL queries:

```
    Raw SQL Query
         │
         ▼
    ┌─────────────────┐
    │  pg_query.Parse │  ← PostgreSQL SQL parser
    └────────┬────────┘
             │ AST
             ▼
    ┌─────────────────┐
    │  Extract table  │
    │  name from FROM │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  Parse column   │
    │  list (SELECT)  │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────────────┐
    │  PatternMatcher         │
    │  • Walk WHERE clause    │
    │  • Extract field=value  │
    │  • Extract field=$1     │
    │  • Build field map      │
    └────────┬────────────────┘
             │
             ▼
    ┌─────────────────────────┐
    │  Table-specific handler │
    │  builder (determines    │
    │  which API to call)     │
    └─────────────────────────┘
```

**Supported WHERE Clause Patterns**:

- Simple equality: `field = 'value'`
- Parameterized: `field = $1`
- Conjunctions: `field1 = 'a' AND field2 = 'b'`

**Unsupported Features** (return high cost in EXPLAIN):

- Joins across tables
- Aggregations (SUM, COUNT, etc.)
- ORDER BY, GROUP BY
- DISTINCT
- Subqueries
- Complex expressions

### 8. Statistics & Query Planning

**Location**: `stats/stats.go`, `explain.go`

The FDW collects runtime statistics to help PostgreSQL make better query planning decisions:

```
┌──────────────────────────────────────────────────────────┐
│              Statistics Collection Flow                  │
└──────────────────────────────────────────────────────────┘

    Query Execution
         │
         ▼
    ┌─────────────────┐
    │  Execute API    │
    │  call and track │
    │  • Cost (ms)    │
    │  • Row count    │
    │  • Row width    │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  stats.Package  │
    │  • Quantile     │
    │    streams      │
    │  • Median calc  │
    └────────┬────────┘
             │
             │
    When EXPLAIN query runs:
             │
             ▼
    ┌─────────────────────────┐
    │  Generate query plan    │
    │  with median costs      │
    │                         │
    │  Foreign Scan on perms  │
    │    Cost: 10.00..50.00   │
    │    Rows: 1000           │
    │    Width: 128           │
    └─────────────────────────┘
```

**Statistics Tracked** (per table + operation):

- **Cost**: Query execution time
- **Rows**: Number of results returned
- **Width**: Average row size in bytes

**Purpose**: PostgreSQL uses EXPLAIN cost estimates to:

1. Choose between local and foreign table scans
2. Decide join strategies
3. Optimize multi-table queries

### 9. Error Handling

**Location**: `common/errors.go`

The FDW uses typed errors to distinguish between different failure modes:

```
┌──────────────────────────────────────┐
│         Error Type Hierarchy         │
└──────────────────────────────────────┘

    fdw.Error (interface)
         │
    ┌────┴────┬──────────┬───────────┐
    │         │          │           │
    ▼         ▼          ▼           ▼
Semantics  Unsupported Query    Internal
  Error      Error     Error      Error
    │          │         │           │
    │          │         │           │
Examples:  Examples:  Examples:  Examples:
• Bad       • JOINs    • Missing  • SpiceDB
  syntax    • GROUP BY   required   API errors
• Invalid   • Subquery   field    • Network
  column    • Complex  • Type       failures
• Cursor      WHERE      mismatch
  not found
```

**Error Flow**:
```
SpiceDB API Error
       │
       ▼
┌──────────────┐
│ Wrap in      │
│ fdw.Error    │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Return to    │
│ wire protocol│
└──────┬───────┘
       │
       ▼
PostgreSQL Client
   sees SQL error
```

### 10. Consistency Control

**Location**: `tables/util.go`

The FDW supports SpiceDB's consistency guarantees through a special `consistency` column:

```
┌─────────────────────────────────────────────┐
│        Consistency Models                   │
└─────────────────────────────────────────────┘

"minimize_latency" (default)
    │
    ├─> Use newest available snapshot
    └─> Lowest latency, may see slight staleness

"fully_consistent"
    │
    ├─> Wait for fully consistent view
    └─> Higher latency, guaranteed fresh

"<zedtoken>"
    │
    ├─> At least as fresh as the provided token's timestamp
    └─> Read-your-writes consistency

"@<zedtoken>"
    │
    ├─> Exact snapshot match (may fail if GC'd)
    └─> Strict point-in-time reads
```

**Usage in Queries**:
```sql
-- Get consistency token from INSERT/DELETE operations via RETURNING
INSERT INTO relationships
  (resource_type, resource_id, relation, subject_type, subject_id)
VALUES
  ('doc', 'readme', 'viewer', 'user', 'alice')
RETURNING consistency;

-- Use returned token in subsequent query for read-your-writes consistency
SELECT * FROM relationships
WHERE ... AND consistency = '<returned-token>';

-- Or request fully consistent reads directly
SELECT resource_id FROM permissions
WHERE ... AND consistency = 'fully_consistent';
```

## Data Flow Examples

### Example 1: CheckPermission Query

```
1. Client sends SQL:
   SELECT has_permission FROM permissions
   WHERE resource_type = 'doc' AND resource_id = 'readme'
     AND permission = 'view' AND subject_type = 'user'
     AND subject_id = 'alice';

2. FDW Proxy:
   ┌──────────────────────────────────┐
   │ Parse SQL → Identify all fields  │
   │ specified                        │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Route to CheckPermission handler │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Build CheckPermissionRequest:    │
   │   resource: doc:readme           │
   │   permission: view               │
   │   subject: user:alice            │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Call SpiceDB API                 │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Response: { permissionship: ... }│
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Format as PostgreSQL row:        │
   │ | has_permission | consistency | │
   │ | true           | GxQv...     | │
   └────────────┬─────────────────────┘
                │
                ▼
3. Client receives result set
```

### Example 2: LookupResources with Cursor

```
1. Client sends:
   BEGIN;
   DECLARE c CURSOR FOR
     SELECT resource_id FROM permissions
     WHERE resource_type = 'doc' AND permission = 'view'
       AND subject_type = 'user' AND subject_id = 'alice'
       AND has_permission = true;

2. FDW Proxy:
   ┌──────────────────────────────────┐
   │ START TRANSACTION                │
   │   → Set withinTransaction = true │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ DECLARE CURSOR                   │
   │   → Parse SELECT statement       │
   │   → Store in session.cursors["c"]│
   │   → Don't execute yet            │
   └──────────────────────────────────┘

3. Client sends:
   FETCH 100 FROM c;

4. FDW Proxy:
   ┌──────────────────────────────────┐
   │ Lookup cursor "c"                │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Determine query type:            │
   │   → LookupResources (no res_id)  │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Build LookupResourcesRequest:    │
   │   subject: user:alice            │
   │   permission: view               │
   │   resource_type: doc             │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Stream SpiceDB API results       │
   │ (yields: doc:1, doc:2, ...)      │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Return first 100 rows            │
   │ Store stream continuation token  │
   │ in cursor.rowsCursor             │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Format as PostgreSQL rows:       │
   │ | resource_id |                  │
   │ | doc:1       |                  │
   │ | doc:2       |                  │
   │ | ...         |                  │
   └──────────────────────────────────┘

5. Client can repeat:
   FETCH 100 FROM c;  -- Get next batch

6. Client ends:
   CLOSE c;
   COMMIT;
```

### Example 3: Insert Relationships

```
1. Client sends SQL:
   INSERT INTO relationships
     (resource_type, resource_id, relation,
      subject_type, subject_id)
   VALUES
     ('doc', 'readme', 'viewer', 'user', 'alice'),
     ('doc', 'readme', 'viewer', 'user', 'bob');

2. FDW Proxy:
   ┌──────────────────────────────────┐
   │ Parse INSERT statement           │
   │   → Extract column names         │
   │   → Extract value tuples         │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Build WriteRelationshipsRequest: │
   │   updates: [                     │
   │     TOUCH doc:readme#viewer@...  │
   │     TOUCH doc:readme#viewer@...  │
   │   ]                              │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Call SpiceDB API                 │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Response: { written_at: ... }    │
   └────────────┬─────────────────────┘
                │
                ▼
   ┌──────────────────────────────────┐
   │ Return INSERT completion         │
   │ "INSERT 0 2"                     │
   └──────────────────────────────────┘

3. Client receives success
```

## Key Design Decisions

### 1. Stateless Query Execution

Each query is independent except for:

- Cursor state (stored in session)
- Transaction boundaries (tracked but **not affecting SpiceDB calls**)

This simplifies the implementation since SpiceDB APIs are already atomic, but does mean if Postgres rolls back
its own transaction, SpiceDB operations will still remain applied.

In the future, we will investigate having the SpiceDB operations also rolled-back.

### 2. Pattern-Based Query Routing

The FDW examines which fields are specified in WHERE clauses to determine which SpiceDB API to call. This is more intuitive than requiring users to call functions or use special syntax.

### 3. Streaming via Cursors

Large result sets (especially LookupResources/LookupSubjects) are streamed using PostgreSQL cursors rather than materializing everything in memory.

### 4. Statistics-Based Query Planning

By collecting runtime statistics and providing them via EXPLAIN, the FDW helps PostgreSQL make intelligent decisions when the foreign tables are used in complex queries.

In the future, these statistics will come directly from the SpiceDB query planner and/or Materialize, further
improving decision making by Postgres.

## Performance Considerations

### Query Pattern Optimization

**Best Performance**:
```sql
-- Specific, targeted queries
SELECT has_permission FROM permissions WHERE ...;  -- CheckPermission
```

**Moderate Performance**:
```sql
-- Streaming with cursors
DECLARE c CURSOR FOR SELECT resource_id FROM permissions WHERE ...;
FETCH 100 FROM c;  -- LookupResources in batches
```

**Avoid**:
```sql
-- Full table scans (not supported)
SELECT * FROM permissions;  -- No WHERE clause

-- Complex queries (not supported)
SELECT * FROM permissions p1 JOIN permissions p2 ...;
```

### Consistency Trade-offs

- `minimize_latency`: Best performance, eventual consistency
- `fully_consistent`: Higher latency, strong consistency
- Specific token: Read-your-writes, consistent views across queries

### Cursor vs Direct SELECT

Use cursors when:

- Result set may be large (>1000 rows)
- Processing results incrementally
- Need to limit memory usage

Use direct SELECT when:

- Small result sets expected
- Simple one-time queries
- No need for result streaming

## Security Model

### Authentication Flow

```
PostgreSQL Client
      │
      │ username, password
      ▼
┌──────────────────┐
│ FDW Proxy        │
│ validateAuth()   │
└────────┬─────────┘
         │ SpiceDB token (from config)
         ▼
┌──────────────────┐
│ SpiceDB Server   │
└──────────────────┘
```

**Two-Layer Authentication**:

1. Client → FDW Proxy: PostgreSQL username/password
2. FDW Proxy → SpiceDB: Bearer token (gRPC)

**Important**: The FDW proxy uses a single SpiceDB token for all client connections. Per-client authorization must be handled at the PostgreSQL level or within your application.

## Future Enhancements

Potential areas for improvement:

1. **Per-Client SpiceDB Tokens**: Map PostgreSQL users to different SpiceDB tokens
2. **Prepared Statement Caching**: Cache parsed query plans
3. **Connection Pooling**: Reuse SpiceDB gRPC connections
4. **Push-down Aggregations**: Support COUNT(*) by calling SpiceDB APIs differently
5. **Schema Mutation**: Support CREATE/ALTER table syntax for schema updates
6. **Batch Operations**: Combine multiple INSERTs/DELETEs into single API calls
7. **Query Hints**: Allow SQL comments to control consistency, caveats, etc.

## Debugging & Observability

### Enable Debug Logging

```bash
spicedb postgres-fdw --log-level debug ...
```

### Useful Log Events

- Query parsing and routing decisions
- SpiceDB API calls and responses
- Cursor creation/fetch/close operations
- Statistics updates

### EXPLAIN Analysis

```sql
-- See query plan and estimated costs
EXPLAIN SELECT * FROM permissions WHERE ...;
```

Helps understand:

- Which operation will be used (CheckPermission, LookupResources, etc.)
- Estimated costs and row counts
- Whether query is supported

## Conclusion

The SpiceDB Postgres FDW proxy provides a powerful bridge between the SQL world and SpiceDB's authorization model. By implementing the PostgreSQL wire protocol and translating queries into appropriate SpiceDB API calls, it enables seamless integration with existing PostgreSQL tools and workflows while maintaining the full power and consistency guarantees of SpiceDB.
