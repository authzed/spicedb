# Query execution profiles

`Current` preserves the normal optimizer and signal pipeline. `NativeParityV1`
selects a fixed, explicit capability set for comparing QP with production native
Check. Its name identifies a target, not an achieved performance result.

For a schema such as:

```zed
definition user {}
definition document {
  relation reader: user
  relation writer: user
  permission view = reader + writer
}
```

A Check of `document:report#view@user:alice` preserves the declared union and
reader/writer order, defers caveat evaluation until the public response boundary,
and retains dispatch caching and singleflight. Registered tree simplification,
alias collapse, caveat pushdown, reachability rewrites, learned branch ordering
and arrow reversal do not run. No observations are collected or learned.

A caveated reader path remains conditional during intermediate dispatch. The
public Check evaluates it using request context and returns the same missing
fields as native when context is incomplete.

Limits are resolved once from deployment settings. Both service and local dispatch
receivers use the same immutable profile. New registered passes cannot enter this
profile implicitly. The fixed Check concurrency pass is explicitly enabled for
union/intersection/exclusion reducers. Remaining native capability coverage is reported separately.
