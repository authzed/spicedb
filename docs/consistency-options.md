# Consistency Options

SpiceDB's [v1 API] provides the ability to specify the *consistency* level at which API calls are made.

Consistency is provided via the [Consistency message] on supported API calls.

[v1 API]: https://buf.build/authzed/api/tree/main/authzed/api/v1
[Consistency message]: https://buf.build/authzed/api/docs/main/authzed.api.v1#authzed.api.v1.Consistency

## Default Consistency Levels

| API                   | Default Consistency |
|-----------------------|---------------------|
| `WriteRelationships`  | `fully_consistent`  |
| `DeleteRelationships` | `fully_consistent`  |
| `ReadSchema`          | `fully_consistent`  |
| `WriteSchema`         | `fully_consistent`  |
| All other APIs        | `minimize_latency`  |

## minimize_latency

`minimize_latency` will attempt to minimize the latency of the API call, using whatever caches are available.

**WARNING:** If used exclusively, this can lead to a window where the [New Enemy Problem] can occur.

[New Enemy Problem]:(https://authzed.com/blog/new-enemies/)

```proto
Consistency {
    minimize_latency: true
}
```

## at_least_as_fresh

`at_least_as_fresh` will ensure that all data used for computing the response is at least as fresh as the point-in-time specified in the [ZedToken/Zookie].

If newer information is available, it will be used.

```proto
Consistency {
    at_least_as_fresh: ZedToken{
        token: "..."
    }
}
```

## at_exact_snapshot

`at_exact_snapshot` will ensure that all data used for computing the response is that found at the *exact* point-in-time specified in the [ZedToken/Zookie].

If the snapshot is not available, an error will be raised.

```proto
Consistency {
    at_exact_snapshot: ZedToken{
        token: "..."
    }
}
```

## fully_consistent

`fully_consistent` will ensure that all data used is fully consistent with the latest data available within the SpiceDB datastore.

Note that the snapshot used will be loaded at the beginning of the API call, and that new data  written *after* the API starts executing will be ignored.

**WARNING:** Use of `fully_consistent` means little caching will be available, which means performance will suffer.
Only use if a [ZedToken/Zookie] is not available or absolutely latest information is required.

[ZedToken/Zookie]: zedtokens-and-zookies.md

```proto
Consistency {
    fully_consistent: true
}
```
