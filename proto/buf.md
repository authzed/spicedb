# The Authzed API

Authzed exposes its APIs via [gRPC] and publishes its [Protobuf Definitions] to the [Buf Registry].

The API can be accessed by [client libraries] or [zed], the Authzed command-line tool.

[gRPC]: https://grpc.io
[Protobuf Definitions]: https://buf.build/authzed/api
[Buf Registry]: https://buf.build
[client libraries]: #client-libraries
[zed]: /reference/zed

## Client Libraries

While any developer can generate their own clients from the public Authzed [Protobuf Definitions], the Authzed team officially supports and maintains client libraries for a variety of languages.
If you are interested in additional language support, feel free to [reach out].

Official clients leverage their language's gRPC ecosystem to provide an abstraction for each version of the Authzed API.
For example, in Go, [constructing a client] is no different from creating any other gRPC connection and can use any of the ecosystem's gRPC DialOption to configure its connection.

The list of the officially maintained client libraries includes:
- [Go](https://github.com/authzed/authzed-go)
- [Python](https://github.com/authzed/authzed-py)
- [Java](https://github.com/authzed/authzed-java) (includes Clojure examples)
- [Node](https://github.com/authzed/authzed-node)
- [Ruby](https://github.com/authzed/authzed-rb)

[reach out]: https://authzed.com/contact
[constructing a client]: https://github.com/authzed/authzed-go#initializing-a-client

## Versions

### Versioning & Deprecation Policy

APIs are versioned by their Protobuf package and version compatibility is enforced by the [Buf Breaking Rules].
New Messages, Fields, Services, and RPCs can be added to existing APIs as long as existing calls continue to exhibit the same behavior.

When an API version is marked as deprecated, the duration of its continued support will be announced.

During this deprecation period, Authzed reserves the right to take various actions to users and clients using deprecated APIs:
- Email users
- Print warning messages from official clients
- Purposely disrupt service near the end of the period

[Buf Breaking Rules]: https://docs.buf.build/breaking-rules

### authzed.api.v1alpha1

The v1 Authzed API is currently **Work In Progress**.

#### API Docs

gRPC Documentation can be found on the [Buf Registry][bsr-v1alpha1].

#### Changes

- A new _Schema_ language replaces Namespace Configs.
  Existing v0 NamespaceConfigs can be read from the SchemaRead() API.
- More to come!

[bsr-v1alpha1]: https://buf.build/authzed/api/docs/main/authzed.api.v1alpha1

### authzed.api.v0

v0 is the first iteration of the Authzed API.
It strived to be as accurate to the description of the [Zanzibar] APIs as possible.

[Zanzibar]: https://research.google/pubs/pub48190/

#### API Docs

gRPC Documentation can be found in the [v0 docs] or the [Buf Registry][bsr-v0].

[v0 docs]: /v0/api
[bsr-v0]: https://buf.build/authzed/api/docs/main/authzed.api.v0

#### Caveats

This API did not originally have a Protobuf package defined and was migrated to have one.
While the API can be called with or without the `authzed.api.v0` prefix, some tools built to use the [gRPC Reflection API] will not work with the prefixless form.
This had no effect on any official tooling or client libraries as all stable releases have always used the fully prefixed names of RPCs.

[gRPC Reflection API]: https://github.com/grpc/grpc/blob/master/doc/server-reflection.md
