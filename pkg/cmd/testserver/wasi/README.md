# SpiceDB testing server for WASI

This builds the SpiceDB testing server, the one run by `spicedb serve-testing`, as a
[WASI](https://wasi.dev) (`wasip1`) reactor module. Test doubles in client libraries can embed it
to answer checks, lookups and writes with SpiceDB itself, in any language with a WebAssembly
runtime, without running a server or a container.

The module serves the `authzed.api.v1` **PermissionsService**, **SchemaService** and
**ExperimentalService**, with the same configuration, limits and errors as `serve-testing`. Each
session has its own isolated datastore, just as each bearer token does in `serve-testing`.

It is a testing tool: its datastore is in memory, and nothing in it is persisted.

## Building

Each SpiceDB release attaches the module as `spicedb-testserver.wasm`. To build it yourself:

```sh
mage build:wasi    # writes dist/spicedb-testserver.wasm
```

or directly:

```sh
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -ldflags="-s -w" \
  -o spicedb-testserver.wasm ./pkg/cmd/testserver/wasi
```

The module is about 80MB (about 14MB compressed). Compiling it is the expensive step, so hosts
should cache the compiled module (for example, wasmtime's compilation cache), and instantiate it
once per process.

## ABI

All requests and responses are serialized protobuf messages from the public `authzed.api.v1`
package, so a client library can reuse its generated protobuf code.

The module imports only `wasi_snapshot_preview1`. Hosts must call `_initialize` once after
instantiating it. It needs the clock (for relationship expiration) and random number generation,
and no filesystem, networking or arguments.

| Export | Signature | Description |
| --- | --- | --- |
| `alloc` | `(size i32) -> i32` | Returns a buffer of `size` bytes in module memory, for the host to write an argument into. |
| `free` | `(ptr i32)` | Releases a buffer from `alloc`, or a result buffer. |
| `session_new` | `(schema_ptr i32, schema_len i32) -> i64` | Creates a session with its own empty datastore. If the schema is non-empty, it is written with `WriteSchema`, and the session is discarded if that fails. The result payload is the session handle, as a little-endian `uint32`. |
| `session_call` | `(handle i32, method_ptr i32, method_len i32, req_ptr i32, req_len i32) -> i64` | Invokes an API method against a session. The method is the full gRPC method name, for example `/authzed.api.v1.PermissionsService/CheckPermission`. |
| `session_free` | `(handle i32)` | Discards a session and its datastore. |

### Results

`session_new` and `session_call` return a result buffer packed into an `i64` as
`(ptr << 32) | len`. The host reads the `len` bytes at `ptr`, and must then `free(ptr)`.

The first byte of the result is a status:

- `0`: success. The rest is the payload.
- `1`: error. The rest is a serialized `google.rpc.Status`, with the same code, message and
  details (such as `ErrorInfo`) that `serve-testing` would return.

### Streaming

Streamed messages are framed: each message is prefixed with its length as a little-endian
`uint32`, and the frames are concatenated.

- For **server-streaming** methods (`LookupResources`, `LookupSubjects`, `ReadRelationships`,
  `ExportBulkRelationships`, ...), the payload is the framed sequence of all response messages.
- For **client-streaming** methods (`ImportBulkRelationships`), the request is the framed
  sequence of request messages.
- For **unary** methods, the request and the payload are each a single, unframed message.

The Watch service is not available, since its calls never complete on their own.

### Example call sequence

1. `ptr = alloc(len(schema))`, write the schema there, `r = session_new(ptr, len(schema))`,
   `free(ptr)`; read the handle from the result, and `free` the result.
2. Write the method name and a serialized `WriteRelationshipsRequest` into buffers from `alloc`,
   and call `session_call(handle, ...)`; free the buffers and the result.
3. Likewise, `CheckPermission`, `LookupResources`, and so on.
4. `session_free(handle)` when the test ends.

ZedTokens and cursors from the module are real, and can be passed back to it.

## Concurrency

WASI preview 1 has no threads: the module runs one call at a time. A host must not call into the
same instance concurrently; use one instance per thread, or serialize calls.

## Examples

- [`example/python`](example/python) has a minimal wasmtime-py host (`spicedb_testserver.py`), and
  a conformance script that runs a fixture against the module and against `spicedb
  serve-testing`, and requires identical results:

  ```sh
  pip install wasmtime authzed
  python example/python/conformance.py --wasm dist/spicedb-testserver.wasm --spicedb ./spicedb
  ```

- [`wasi_test.go`](wasi_test.go) hosts the module in [wazero](https://wazero.io) (`mage test:wasi`).

Go code can use the server without WebAssembly, through
[`pkg/cmd/testserver/inprocess`](../inprocess).

## Versioning

The ABI is new, and may change before it is declared stable. The module evaluates exactly as the
SpiceDB version it was built from; client libraries should pin a version and say which.
