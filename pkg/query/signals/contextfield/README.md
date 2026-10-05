# contextfield

Request-context stability by field name, at the exact request revision. Missing and explicit null are distinct at the input adapter. Track counts, all-equal and a bounded dominant-frequency lower bound using at most 16 fingerprints. Overflow never becomes evidence of stability. No raw values are retained. This is input evidence, not an authorization result or effective relationship caveat context.

Registered through the explicit signal catalog. Access with typed Ref/Input values.
Evidence remains in its original schema/revision view. Colocated unit tests and
Rapid properties compare the estimate with an exact independent counting model.
Run `go test -race ./pkg/query/signals/contextfield`.

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

Summarized request: `CheckPermission(document:report, view, user:alice,
context={ "region": "east" })` at revision `R1`. At validated request ingress,
the adapter selects `region` from the caveat parameter names and publishes once
for this logical request. The key is the field name `region`, rather than the
caveat, object, or subtree. Other caveats using that field share this population
within the same schema and revision.

Each row below starts with an empty `region` population:

| Scenario at `R1` | What the signal stores |
| --- | --- |
| Four requests all supply `"east"` | One fingerprint with frequency 4; `Samples=4`, `Missing=0`, `AllEqual=true`, `DominantShareLowerBound=1` |
| Three requests supply `"east"` and one supplies `"west"` | Two fingerprints; `Samples=4`, `AllEqual=false`, `DominantShareLowerBound=0.75` |
| Two requests supply `"east"`, one omits `region` | `Samples=3`, `Missing=1`, `AllEqual=true`, `DominantShareLowerBound=1` among the two present values |
| One request explicitly supplies null | A present null fingerprint: `Samples=1`, `Missing=0`; it differs from an absent field |
| Seventeen requests supply seventeen distinct strings | Sixteen retained fingerprints; `Samples=17`, `Overflow=1`, `AllEqual=false`, `DominantShareLowerBound=1/17` |
| Encoding fails or the request exceeds a sampling budget | The adapter skips that field; no observation is stored |

Only process-keyed fingerprints and counters are retained, not the context values.
`AllEqual` refers to present values; a consumer must inspect `Missing` as well.
The provider's `RevisionBound` policy starts a separate field population at `R2`.
These are stability observations for a future caveat-placement optimization;
the implementation does not yet change caveat placement based on them. Explicit
null here illustrates input sampling, not whether that context satisfies the
caveat's parameter type.
