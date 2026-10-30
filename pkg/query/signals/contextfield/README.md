# Context-field stability

## Summary

Tracks whether each request-context field keeps the same value.

- **Revision scope:** Exact datastore revision.
- **Aging:** No time-based aging; storage limits apply.

## Description

Some context values stay the same across many requests, while others change
frequently. This signal tracks that pattern separately for each context field.
An optimization can use that history to decide whether doing work for a particular
context value early is likely to pay off.

It compares values without keeping the values themselves. An absent field is
tracked separately from a supplied value, so a field that is usually missing does
not accidentally look reliable.

## Example

Summarized schema:

```zed
definition user {}
caveat in_region(region string) { region == "east" }
definition document {
    relation reader: user with in_region
    permission view = reader
}
```

A request checks whether Alice can view `document:report`, supplying
`{ "region": "east" }`. Requests using the same schema and datastore revision
contribute to the same history for `region`, even when they check different
objects or use different caveats that read that field.

Each row below starts with an empty history:

| Requests | What the signal learns |
| --- | --- |
| Four requests supply `"east"` | Every supplied value has been the same |
| Three supply `"east"` and one supplies `"west"` | The value varies; the most common value accounts for 75% of supplied values |
| Two supply `"east"` and one omits `region` | The supplied values agree, but one request had no value |
| One explicitly supplies null | A value was supplied; this is different from omitting the field |
| Seventeen requests supply seventeen different strings | The field is changing; reaching the tracking limit does not make it look stable |
| A value cannot be sampled within the request budget | That request contributes no observation for the field |

A different datastore revision starts a fresh history. These observations can
inform a later context-placement decision; this signal alone does not make that
decision.

## Details

The key is the context field name, not the caveat, object or subtree. The provider
uses `RevisionBound`, so histories are isolated by schema and exact selected
revision. It does not choose or quantize the revision.

Observations distinguish missing fields from explicit null at the input adapter.
Only process-keyed value fingerprints and counters are retained. This describes
request input, not the effective context after combining it with relationship
caveat data, and not whether a value satisfies a caveat's parameter type.

The estimate includes `Samples`, `Missing`, `Overflow`, `AllEqual`, and
`DominantShareLowerBound`. At most 16 distinct fingerprints are retained. Additional
values increase `Overflow` and rule out `AllEqual`. Retained fingerprint strings
own their backing storage, including when a repeated value updates a counter.

`AllEqual` considers present values only; consumers must also inspect `Missing`.
For example, two `"east"` values and one omission produce `Samples=3`, `Missing=1`,
`AllEqual=true`, and `DominantShareLowerBound=1`. Seventeen distinct present values
produce `Samples=17`, `Overflow=1`, `AllEqual=false`, and a dominant-share lower
bound of `1/17`.

Unit tests and Rapid properties compare estimates with an independent counting
model. Run `go test -race ./pkg/query/signals/contextfield`.
