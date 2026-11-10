# Typed planning hints

Hints are strong recommendations with a typed payload, target, producer and compact
evidence references. ArrowDirection is consumed at compilation; BranchOrder is
consumed by a planning optimization. They never substitute for permission semantics.

The ledger requires structured reasons and consumer identity for declines,
supersessions and invalidations. Conflicts need explicit producer precedence;
identical recommendations coalesce while preserving provenance. Unhandled hints
are recorded as Unconsumed. Validation is always active; an optional trace sink
collects decision records for future debug presentation.

Supporting `Hint.Evidence` is collected and retained only when the ledger has a
trace sink. Producers check `Context.HintEvidenceEnabled()` before building
references; the ledger also discards evidence supplied by other producers when
tracing is disabled. Payloads, targets, producer precedence, and structured
accept/decline accounting remain active in both modes. Signal-store evidence
metadata still supports revision/schema lifecycle and aging independently.
