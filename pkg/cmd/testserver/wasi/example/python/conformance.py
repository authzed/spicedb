"""Runs a fixture against the WASI module and against `spicedb serve-testing`, and compares them.

    python conformance.py --wasm spicedb-testserver.wasm --spicedb ./spicedb

Every step's response (or error code and message) must be identical, after clearing the
revision-dependent fields (ZedTokens and cursors). Streamed responses are compared as sets,
since their order is unspecified.
"""

import argparse
import socket
import subprocess
import sys
import time
import uuid

import grpc
from authzed.api.v1 import (
    CheckBulkPermissionsRequest,
    CheckBulkPermissionsRequestItem,
    CheckPermissionRequest,
    Consistency,
    ContextualizedCaveat,
    ExpandPermissionTreeRequest,
    InsecureClient,
    LookupResourcesRequest,
    LookupSubjectsRequest,
    ObjectReference,
    ReadRelationshipsRequest,
    ReadSchemaRequest,
    Relationship,
    RelationshipFilter,
    RelationshipUpdate,
    SubjectReference,
    WriteRelationshipsRequest,
    WriteSchemaRequest,
)
from google.protobuf import struct_pb2, timestamp_pb2

from spicedb_testserver import TestServerError, TestServerModule

SCHEMA = """
use expiration

caveat only_on(day string, allowed string) {
    day == allowed
}

caveat tiered(ip string, tier string, required_tier string) {
    ip != "" && tier == required_tier
}

definition user {}

definition group {
    relation member: user | group#member
}

definition organization {
    relation admin: user
    relation member: user | group#member
    permission manage = admin
    permission view = member + manage
}

definition workspace {
    relation organization: organization
    relation viewer: user
    permission manage = organization->manage
    permission view = viewer + organization->view
}

definition document {
    relation workspace: workspace
    relation org: organization
    relation viewer: user | user:* | group#member | user with only_on | user with tiered
    relation editor: user with expiration
    relation banned: user
    permission edit = editor + workspace->manage
    permission view = (viewer + edit + workspace->view) - banned
    permission org_view = viewer & org->view
    permission all_orgs_manage = org.all(manage)
    permission any_org_view = org.any(view)
}
"""

FULLY = Consistency(fully_consistent=True)


def obj(ref):
    typ, oid = ref.split(":")
    return ObjectReference(object_type=typ, object_id=oid)


def subj(ref):
    ref, _, relation = ref.partition("#")
    return SubjectReference(object=obj(ref), optional_relation=relation)


def rel(resource, relation, subject, caveat=None, expires=None):
    r = Relationship(resource=obj(resource), relation=relation, subject=subj(subject))
    if caveat:
        name, context = caveat
        ctx = struct_pb2.Struct()
        ctx.update(context)
        r.optional_caveat.CopyFrom(ContextualizedCaveat(caveat_name=name, context=ctx))
    if expires:
        ts = timestamp_pb2.Timestamp()
        ts.FromJsonString(expires)
        r.optional_expires_at.CopyFrom(ts)
    return RelationshipUpdate(operation=RelationshipUpdate.OPERATION_CREATE, relationship=r)


def check(resource, permission, subject, context=None):
    req = CheckPermissionRequest(
        consistency=FULLY, resource=obj(resource), permission=permission, subject=subj(subject)
    )
    if context is not None:
        req.context.update(context)
    return req


RELATIONSHIPS = [
    rel("organization:acme", "admin", "user:olivia"),
    rel("organization:acme", "member", "group:eng#member"),
    rel("group:eng", "member", "user:alice"),
    rel("group:eng", "member", "group:interns#member"),
    rel("group:interns", "member", "user:ivan"),
    rel("workspace:ws1", "organization", "organization:acme"),
    rel("workspace:ws1", "viewer", "user:wendy"),
    rel("document:plan", "workspace", "workspace:ws1"),
    rel("document:plan", "org", "organization:acme"),
    rel("document:plan", "viewer", "group:eng#member"),
    rel("document:plan", "banned", "user:ivan"),
    rel("document:plan", "editor", "user:ed", expires="2300-01-01T00:00:00Z"),
    rel("document:plan", "editor", "user:gone", expires="2000-01-01T00:00:00Z"),
    rel("document:public", "viewer", "user:*"),
    rel("document:weekly", "viewer", "user:carol", caveat=("only_on", {"allowed": "monday"})),
    rel("document:gold", "viewer", "user:dave", caveat=("tiered", {"required_tier": "gold"})),
    rel("group:loop1", "member", "group:loop2#member"),
    rel("group:loop2", "member", "group:loop1#member"),
]

STEPS = [
    ("invalid schema", "WriteSchema", WriteSchemaRequest(schema="definition user {")),
    ("write schema", "WriteSchema", WriteSchemaRequest(schema=SCHEMA)),
    ("read schema", "ReadSchema", ReadSchemaRequest()),
    ("write relationships", "WriteRelationships", WriteRelationshipsRequest(updates=RELATIONSHIPS)),
    ("create conflict", "WriteRelationships", WriteRelationshipsRequest(updates=RELATIONSHIPS[:1])),
    # The organization/workspace trial scenario: manage = admin, and arrows from children.
    ("org manage = admin", "CheckPermission", check("organization:acme", "manage", "user:olivia")),
    ("workspace arrow to org manage", "CheckPermission", check("workspace:ws1", "manage", "user:olivia")),
    ("document arrow chain", "CheckPermission", check("document:plan", "edit", "user:olivia")),
    ("workspace viewer via arrow", "CheckPermission", check("document:plan", "view", "user:wendy")),
    ("union via nested group", "CheckPermission", check("document:plan", "view", "user:alice")),
    ("exclusion", "CheckPermission", check("document:plan", "view", "user:ivan")),
    ("intersection", "CheckPermission", check("document:plan", "org_view", "user:alice")),
    ("arrow all", "CheckPermission", check("document:plan", "all_orgs_manage", "user:olivia")),
    ("arrow any", "CheckPermission", check("document:plan", "any_org_view", "user:ivan")),
    ("no permission", "CheckPermission", check("document:plan", "view", "user:mallory")),
    ("wildcard", "CheckPermission", check("document:public", "view", "user:anyone")),
    ("expiring", "CheckPermission", check("document:plan", "edit", "user:ed")),
    ("expired", "CheckPermission", check("document:plan", "edit", "user:gone")),
    ("caveat missing context", "CheckPermission", check("document:weekly", "view", "user:carol")),
    ("caveat full context true", "CheckPermission", check("document:weekly", "view", "user:carol", {"day": "monday"})),
    ("caveat full context false", "CheckPermission", check("document:weekly", "view", "user:carol", {"day": "friday"})),
    ("caveat partial context", "CheckPermission", check("document:gold", "view", "user:dave", {"ip": "10.0.0.1"})),
    ("invalid caveat context", "CheckPermission", check("document:weekly", "view", "user:carol", {"day": 42})),
    ("depth exceeded", "CheckPermission", check("group:loop1", "member", "user:nobody")),
    ("unknown definition", "CheckPermission", check("folder:f", "view", "user:alice")),
    ("unknown permission", "CheckPermission", check("document:plan", "nope", "user:alice")),
    ("bulk check", "CheckBulkPermissions", CheckBulkPermissionsRequest(consistency=FULLY, items=[
        CheckBulkPermissionsRequestItem(resource=obj("document:plan"), permission="view", subject=subj("user:alice")),
        CheckBulkPermissionsRequestItem(resource=obj("document:weekly"), permission="view", subject=subj("user:carol")),
        CheckBulkPermissionsRequestItem(resource=obj("folder:f"), permission="view", subject=subj("user:alice")),
    ])),
    ("lookup resources", "LookupResources", LookupResourcesRequest(
        consistency=FULLY, resource_object_type="document", permission="view", subject=subj("user:carol"))),
    ("lookup resources via groups", "LookupResources", LookupResourcesRequest(
        consistency=FULLY, resource_object_type="document", permission="view", subject=subj("user:alice"))),
    ("lookup subjects", "LookupSubjects", LookupSubjectsRequest(
        consistency=FULLY, resource=obj("document:plan"), permission="view", subject_object_type="user")),
    ("read relationships", "ReadRelationships", ReadRelationshipsRequest(
        consistency=FULLY, relationship_filter=RelationshipFilter(resource_type="document"))),
    ("expand", "ExpandPermissionTree", ExpandPermissionTreeRequest(
        consistency=FULLY, resource=obj("document:plan"), permission="view")),
]


def clear_revision_fields(msg):
    for field, value in msg.ListFields():
        if field.message_type is None:
            continue
        if field.message_type.full_name in ("authzed.api.v1.ZedToken", "authzed.api.v1.Cursor"):
            msg.ClearField(field.name)
        elif hasattr(value, "ListFields"):
            clear_revision_fields(value)
        elif field.message_type.GetOptions().map_entry:
            for item in value.values():
                if hasattr(item, "ListFields"):
                    clear_revision_fields(item)
        else:
            for item in value:
                clear_revision_fields(item)
    return msg


def normalize(result):
    if isinstance(result, list):
        return sorted(str(clear_revision_fields(m)) for m in result)
    return str(clear_revision_fields(result))


class WasiBackend:
    def __init__(self, module):
        self.session = module.new_session()

    def run(self, method, request):
        try:
            return ("ok", normalize(self.session.call(method, request)))
        except TestServerError as e:
            return ("error", e.code, e.message)


class ServeTestingBackend:
    def __init__(self, endpoint):
        self.client = InsecureClient(endpoint, str(uuid.uuid4()))

    def run(self, method, request):
        try:
            result = getattr(self.client, method)(request)
            if not hasattr(result, "SerializeToString"):  # a server stream
                result = list(result)
            return ("ok", normalize(result))
        except grpc.RpcError as e:
            return ("error", e.code().value[0], e.details())


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_serve_testing(binary):
    port = free_port()
    proc = subprocess.Popen(
        [binary, "serve-testing", "--grpc-addr", f"127.0.0.1:{port}", "--readonly-grpc-enabled=false",
         "--log-level", "warn"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    endpoint = f"127.0.0.1:{port}"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            grpc.channel_ready_future(grpc.insecure_channel(endpoint)).result(timeout=1)
            return proc, endpoint
        except grpc.FutureTimeoutError:
            pass
    proc.kill()
    raise RuntimeError("serve-testing did not start")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wasm", required=True)
    parser.add_argument("--spicedb", required=True, help="path to the spicedb binary")
    args = parser.parse_args()

    start = time.perf_counter()
    module = TestServerModule(args.wasm)
    print(f"module load (compile or cache hit + instantiate): {time.perf_counter() - start:.2f}s")

    proc, endpoint = start_serve_testing(args.spicedb)
    failures = 0
    try:
        wasi, reference = WasiBackend(module), ServeTestingBackend(endpoint)
        for name, method, request in STEPS:
            got, want = wasi.run(method, request), reference.run(method, request)
            ok = got == want
            failures += not ok
            summary = want[0] if want[0] == "ok" else f"error {want[1]}"
            print(f"{'PASS' if ok else 'FAIL'}  {name:32} {summary}")
            if not ok:
                print(f"    wasi:          {got}\n    serve-testing: {want}")
    finally:
        proc.kill()

    # Per-call latency against the module.
    session = module.new_session(SCHEMA)
    session.WriteRelationships(WriteRelationshipsRequest(updates=RELATIONSHIPS))
    req = check("document:plan", "view", "user:alice")
    n = 500
    start = time.perf_counter()
    for _ in range(n):
        session.CheckPermission(req)
    print(f"CheckPermission latency: {(time.perf_counter() - start) / n * 1e6:.0f}µs per call ({n} calls)")
    start = time.perf_counter()
    for _ in range(100):
        module.new_session(SCHEMA).close()
    print(f"session create + schema write + close: {(time.perf_counter() - start) / 100 * 1e3:.1f}ms")

    print(f"\n{len(STEPS) - failures}/{len(STEPS)} steps identical")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
