# Observed arrow direction

Registered name: `observed-arrow-direction`. Factory: `New() optimization.Optimizer`.

This optimization recommends which end of an ordinary arrow to enumerate first.
Starting with the smaller population can reduce the number of follow-up checks.
It uses observed raw path-count means to estimate that work, while leaving permission
evaluation to the arrow iterator. A prediction changes execution direction, never
the permission expression or the decision about whether a path grants access.

## Recommendation

For each ordinary two-child arrow, compare these populations:

| Direction | Starting population | Follow-up work |
| --- | --- | --- |
| Left to right | Subjects enumerated by the left subtree | Check the right subtree for each intermediate object |
| Right to left | Resources enumerated by the right subtree | Check the left subtree for each intermediate object |

Recommend right to left when its estimated mean is strictly smaller. Ties retain
left-to-right execution. The pass emits a typed `ArrowDirection` hint for the arrow
occurrence; it does not rewrite the outline. The iterator compiler applies the hint
and records its disposition. Intersection arrows (`all()`) are excluded because
their semantics differ from an ordinary arrow.

## Inputs and signals

Each starting side uses `signal.LookupFirst` with this preference order:

1. [`result-size-at-revision`](../../signals/resultsize/README.md): completed raw
   path-count evidence at the request-selected datastore revision.
2. [`historical-result-size`](../../signals/historicalresultsize/README.md): aged
   evidence across revisions when the exact-revision estimate is unavailable.
3. A static fanout estimate of 3 when neither signal is available.

The fallback is chosen independently for each side. Signal lookup errors propagate;
unavailable estimates are skipped. The pass currently compares means rather than
distribution quantiles. Path counts can include duplicates and conditional paths,
so they estimate iterator work rather than distinct entities.

The caller must supply `Context.Key`. Production planning supplies the structural
identity, operation, request filters, invocation mode, execution strategy, dispatch
role, and dependency footprint needed to match observed populations. The signal
view supplies the schema and revision scope. Test fixtures provide their own builder
in the shared optimizer harness; the production pass has no default key builder.

## Example

Summarized schema:

```zed
definition user {}
definition group {
    relation member: user
}
definition document {
    relation group: group
    permission view = group->member
}
```

Summarized request: `CheckPermission(document:report, view, user:alice)` at revision
`R1`. For example, `document:report#group@group:editors` and
`group:editors#member@user:alice` provide one path granting access.

Forward execution enumerates groups attached to `document:report`, then checks
whether Alice is a member of each group. Reverse execution enumerates groups
containing Alice, then checks whether each group is attached to the document.
The means below summarize prior completed enumerations of the matching populations;
they are estimates, not known sizes for this particular document or user.

| Scenario | Left starting mean | Right starting mean | Emitted recommendation |
| --- | --- | --- | --- |
| Few attached groups, many groups containing the subject; both signals are exact | 2 | 20 | `LeftToRight` |
| Many attached groups, few groups containing the subject; both signals are exact | 80 | 3 | `RightToLeft` |
| Equal exact estimates | 10 | 10 | `LeftToRight` |
| New revision: left uses historical evidence, right has exact evidence | 30 | 4 | `RightToLeft` |
| Neither side has usable exact or historical evidence | Default 3 | Default 3 | `LeftToRight` |
| Left has a completed-zero estimate, right has no usable evidence | 0 | Default 3 | `LeftToRight`; the arrow still executes |

An expired historical estimate is unavailable and uses the next fallback. A mean
of zero never prunes the arrow: data at this request's revision can differ from
the observations, and the permission must still be evaluated.

## Hints and debug evidence

The optimizer stores no statistics itself; the signal providers own those values.
It emits a target occurrence ID and direction payload. The pipeline binds the
producer and structural fingerprint, so a later rewrite can invalidate a stale hint.

With a trace sink configured, each side that used a signal adds an evidence reference
containing the chosen signal name, snapshot ID, and historical flag. Static defaults
have no evidence reference. Without tracing, references are neither built by this
producer nor retained by the ledger. The same direction hint is still consumed and
accounted for. Evidence describes the recommendation; it never changes its application.

## Ordering and expected performance

The production decision phase runs after structural optimizations and dispatch
wrapping. Estimated smaller fanout may save checks, but means omit per-branch cost
and short-circuit effects. Lookup and hint processing add planning overhead;
whole-query performance gains depend on the workload and are not established by
the semantic tests.

## Tests

Unit tests verify exact and historical selection, trace-enabled evidence, omission
of evidence without tracing, compiler consumption in both modes, and the required
key-builder contract. The shared Rapid harness compares Check, IterResources, and
IterSubjects semantics with deliberately misleading predictions. Composed pipeline
properties also combine arrow recommendations with branch reordering.

```sh
go test -race ./pkg/query/queryopt/arrowdirection
go test ./pkg/query/queryopt/arrowdirection -run TestSemanticEquivalence -rapid.checks=1000
```

Rapid reports replay seeds and minimized failures. The shared runner is documented
in [queryopttest](../../../../internal/queryopttest/README.md).
