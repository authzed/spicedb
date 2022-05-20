# CockroachDB Datastore

CockroachDB is a Spanner-like datastore supporting global, immediate consistency, with the mantra "no stale reads."
The CockroachDB implementation should be used when your SpiceDB service runs in multiple geographic regions, and Google's Cloud Spanner is unavailable (e.g. AWS, Azure, bare metal.)

## Implementation Caveats

In order to prevent the new-enemy problem, we need to make related transactions overlap.
We do this by choosing a common database key and writing to that key with all relationships that may overlap.
This tradeoff is cataloged in our blog post [The One Crucial Difference Between Spanner and CockroachDB](https://authzed.com/blog/prevent-newenemy-cockroachdb/).
