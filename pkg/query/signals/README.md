# Planning signals

Signals store observations that can inform query-planner choices. Each provider
owns a named definition, typed population key, observation and estimate, and an
accumulator. This package supplies registration, bounded storage and scoped reads;
it does not change query execution or enable a particular optimization.

## Registration and scope

Construct descriptors with `signal.NewSource` or `signal.NewDerived`, register
all dependencies, and create one `Owner` for the datalayer using `NewOwner`.
Registration rejects duplicate names, incompatible bindings and dependency cycles.
Freeze a registry before sharing it concurrently. Provider callbacks must follow
the accumulator contract: clones and returned values are independent snapshots.

Bind the owner to a loaded stored schema and the request's selected datastore
revision. A provider explicitly chooses `RevisionBound` or `CrossRevision`:

- Revision-bound evidence stays at that exact revision, including a quantized
  revision selected for checks. Neighboring revisions do not share that evidence.
- Cross-revision evidence can be reused within the same schema. Its provider owns
  aging and availability; storage does not infer how much the data changed.
- On a schema change, only cross-revision providers with an explicit compatibility
  callback can transfer evidence. `signal.Unchanged` compares their declared
  dependencies against the schema diff. A transfer may retain, discount or reset
  evidence. It preserves observation times but starts a distinct publication epoch.

For example, checks of `document:report#view@user:alice` at revision R may record
an observation under a population key for the `document#view` subtree. A second
subject can contribute to the same population if the provider's key excludes the
subject. A revision-bound lookup at R+1 is missing; a cross-revision provider may
return historical evidence. Adding an unrelated schema definition can preserve
that historical evidence, while changing a declared dependency resets it.

Consumers use `signal.Observe(view, input, key, observation)` and
`signal.Lookup(view, reference, key)`. Check `estimate.IsAvailable()` before using
the value. Missing, expired and disabled evidence are distinct from a measured
zero. `LookupFirst` tries compatible references in order and reports which one
supplied the first available estimate; a lookup error stops that search.

`view.ForPlan()` memoizes each population's first independent snapshot for one
planning run. Further observations or store eviction cannot change that snapshot.
Different populations are read independently, not as one transactional snapshot.

## Ownership and bounds

Defaults are 16 MiB total estimated resident storage, 256 KiB per entry, four
schemas, 64 exact revisions per schema, and 64 weak owner handles per stored
schema. Options can override these limits. Storage accounts for retained schemas,
entry and key storage, dependency footprints, and accumulator-reported bytes.
Providers must include all their retained state in `Size()`.

Keys support scalar values, strings, and arrays or structs recursively containing
those values. Struct fields must be exported. Pointers, interfaces, channels and
other reference graphs are rejected, as are nonreflexive values such as NaN.
Retained keys and footprint strings are copied so a substring cannot retain an
unaccounted backing buffer. Footprint slices are copied before retention.

Oversized or evicted evidence becomes unavailable; it does not affect permission
semantics. The schema cache holds only bounded weak owner references. Owners and
schemas do not leak learning state between independent datalayers. Legacy readers
can use an isolated local view instead of a shared stored-schema attachment.

Derived providers consume accepted source increments in dependency order, not
cumulative snapshots. Source publication is serialized through derived delivery.
They cannot be written directly through the public observation API.

## Tests

Package tests use synthetic providers and stored-schema fixtures to cover scope
isolation, publication, snapshots, eviction, schema compatibility and concurrency.
The shared `internal/querysignalstest` helpers provide owner and schema
builders for provider-specific tests. Numeric distribution helpers are tested in
`signal` without depending on a built-in provider.

Time-sensitive tests use `testing/synctest` to control time without a custom clock.

## Built-in providers

`NewDefaultRegistry` registers the following providers explicitly;
`NewDefaultOwner` constructs their isolated bounded store.

| Provider | Scope | Observation |
| --- | --- | --- |
| [Context fields](contextfield/README.md) | Exact revision | Presence and value fingerprints by context field name |
| [Result size](resultsize/README.md) | Exact revision | Completed enumeration path counts |
| [Historical result size](historicalresultsize/README.md) | Cross revision | Aged result-size observations with configurable windows |
| [Branch outcomes](branchoutcome/README.md) | Exact revision | Completed Check grants, denials and conditional outcomes |

Each provider package includes examples, unit tests and Rapid properties. These
providers estimate observed populations; their predictions are never permission
proofs. They do not issue datastore queries. Collecting observations during checks
and applying the estimates to optimization decisions are subsequent layers.
