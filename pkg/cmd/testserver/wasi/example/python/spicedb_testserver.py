"""A minimal Python host for the SpiceDB testing server WASI module.

This mirrors what a client library's test double would do: load the module once per process,
create a session per test, and forward authzed.api.v1 requests to it.

    module = TestServerModule("spicedb-testserver.wasm")
    session = module.new_session(schema)
    session.WriteRelationships(WriteRelationshipsRequest(...))
    resp = session.CheckPermission(CheckPermissionRequest(...))
    results = session.LookupResources(LookupResourcesRequest(...))  # a list
"""

import struct

import wasmtime
from authzed.api.v1 import experimental_service_pb2, permission_service_pb2, schema_service_pb2
from google.protobuf import message_factory
from google.rpc import status_pb2

_RESULT_OK = 0

# Method name -> (full gRPC method name, client streaming, server streaming, response class).
_METHODS = {}
for _module in (permission_service_pb2, schema_service_pb2, experimental_service_pb2):
    for _service in _module.DESCRIPTOR.services_by_name.values():
        for _method in _service.methods:
            _METHODS[_method.name] = (
                f"/{_service.full_name}/{_method.name}",
                _method.client_streaming,
                _method.server_streaming,
                message_factory.GetMessageClass(_method.output_type),
            )


class TestServerError(Exception):
    """An error returned by the testing server, as a google.rpc.Status."""

    def __init__(self, status):
        super().__init__(f"{status.code}: {status.message}")
        self.status = status
        self.code = status.code
        self.message = status.message


def _frame(messages):
    return b"".join(struct.pack("<I", len(m)) + m for m in messages)


def _unframe(buf):
    messages, offset = [], 0
    while offset < len(buf):
        (size,) = struct.unpack_from("<I", buf, offset)
        offset += 4
        messages.append(bytes(buf[offset : offset + size]))
        offset += size
    return messages


class TestServerModule:
    """The WASI module, instantiated once. Not thread-safe: use one per thread, or a lock."""

    def __init__(self, wasm_path, cache=True):
        config = wasmtime.Config()
        # Caches the compiled module on disk; compiling it is far slower than instantiating it.
        config.cache = cache
        engine = wasmtime.Engine(config)
        self._store = wasmtime.Store(engine)
        self._store.set_wasi(wasmtime.WasiConfig())
        linker = wasmtime.Linker(engine)
        linker.define_wasi()
        module = wasmtime.Module.from_file(engine, wasm_path)
        instance = linker.instantiate(self._store, module)
        exports = instance.exports(self._store)
        exports["_initialize"](self._store)
        self._memory = exports["memory"]
        self._alloc = exports["alloc"]
        self._free = exports["free"]
        self._session_new = exports["session_new"]
        self._session_call = exports["session_call"]
        self._session_free = exports["session_free"]

    def new_session(self, schema=""):
        """Creates a session with its own empty datastore, writing schema if given."""
        handle = struct.unpack("<I", self._call(self._session_new, schema.encode()))[0]
        return Session(self, handle)

    def _write(self, data):
        ptr = self._alloc(self._store, len(data))
        if data:
            self._memory.write(self._store, data, ptr)
        return ptr

    def _call(self, fn, *buffers, handle=None):
        ptrs = [self._write(b) for b in buffers]
        args = [] if handle is None else [handle]
        for ptr, b in zip(ptrs, buffers):
            args += [ptr, len(b)]
        try:
            packed = fn(self._store, *args) & 0xFFFFFFFFFFFFFFFF
        finally:
            for ptr in ptrs:
                self._free(self._store, ptr)

        ptr, length = packed >> 32, packed & 0xFFFFFFFF
        result = bytes(self._memory.read(self._store, ptr, ptr + length))
        self._free(self._store, ptr)
        if result[0] != _RESULT_OK:
            status = status_pb2.Status()
            status.ParseFromString(result[1:])
            raise TestServerError(status)
        return result[1:]


class Session:
    """An isolated datastore. API methods are available by name, e.g. session.CheckPermission(req).

    Server-streaming methods return a list of responses; client-streaming methods take a list of
    requests.
    """

    def __init__(self, module, handle):
        self._module = module
        self._handle = handle

    def call(self, method, request):
        full_name, client_streaming, server_streaming, response_cls = _METHODS[method]
        if client_streaming:
            payload = _frame([r.SerializeToString() for r in request])
        else:
            payload = request.SerializeToString()
        result = self._module._call(
            self._module._session_call, full_name.encode(), payload, handle=self._handle
        )
        if server_streaming:
            return [response_cls.FromString(m) for m in _unframe(result)]
        return response_cls.FromString(result)

    def __getattr__(self, method):
        if method not in _METHODS:
            raise AttributeError(method)
        return lambda request: self.call(method, request)

    def close(self):
        self._module._session_free(self._module._store, self._handle)
