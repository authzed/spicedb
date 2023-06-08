# CockroachDB Datastore

CockroachDB is a Spanner-like datastore supporting global, immediate consistency, with the mantra "no stale reads."
The CockroachDB implementation should be used when your SpiceDB service runs in multiple geographic regions, and Google's Cloud Spanner is unavailable (e.g. AWS, Azure, bare metal.)

## Implementation Caveats

In order to prevent the new-enemy problem, we need to make related transactions overlap.
We do this by choosing a common database key and writing to that key with all relationships that may overlap.
This tradeoff is cataloged in our blog post [The One Crucial Difference Between Spanner and CockroachDB](https://authzed.com/blog/prevent-newenemy-cockroachdb/).

## Overlap Strategies

There are three transaction overlap strategies:

- `insecure`, which does not protect against the new enemy problem
- `static`, which protects all writes from the new enemy problem
- `request`, which protects all writes with the same [request metadata key](https://github.com/authzed/authzed-go/blob/d97cfb41027742d347391f583dd9c6d1d03ae32b/pkg/requestmeta/requestmeta.go#L26-L30).
- `prefix`, which protects all writes with the same object prefix from the new enemy problem

Depending on your application, `insecure` may be acceptable, and it avoids the performance cost associated with the `static` and `prefix` options.

## When is `insecure` overlap a problem?

Using `insecure` overlap strategy for SpiceDB with CockroachDB means that it is _possible_ that timestamps for two subsequent writes will be out of order.
When this happens, it's _possible_ for the [New Enemy Problem](https://authzed.com/blog/prevent-newenemy-cockroachdb/) to occur.

Let's look at how likely this is, and what the impact might actually be for your workload.

## When can timestamps be reversed?

Before we look at how this can impact an application, let's first understand when and how timestamps can be reversed in the first place.

- When two writes are made in short succession against CockroachDB
- And those two writes hit two different gateway nodes
- And the CRDB gateway node clocks have a delta `D`
- And the writes touch disjoint sets of relationships
- And those two writes are sent within the time delta `D` between the gateway nodes
- And the writes land in ranges whose followers are disjoint sets of nodes
- And other independent cockroach processes (heartbeats, etc) haven't coincidentally synced the gateway node clocks during the writes.

Then it's possible that the second write will be assigned a timestamp earlier than the first write. In the next section we'll look at whether that matters for your application, but for now let's look at what makes the above conditions more or less likely:

- **Clock skew**. A larger clock skew gives a bigger window in which timestamps can be reversed. But note that CRDB enforces a max offset between clocks, and getting within some fraction of that max offset will kick the node from the cluster.
- **Network congestion**, or anything that interferes with node heartbeating. This increases the length of time that clocks can be desynchronized befor Cockroach notices and syncs them back up.
- **Cluster size**. When there are many nodes, it is more likely that a write to one range will not have follower nodes that overlap with the followers of a write to another range. It also makes it more likely that the two writes will have different gateway nodes. On the other side, a 3 node cluster with `replicas: 3` means that all writes will sync clocks on all nodes.
- **Write rate**. If the write rate is high, it's more likely that two writes will hit the conditions to have reversed timestamps. If writes only happen once every max offset period for the cluster, it's impossible for their timestamps to be reversed.

The likelihood of a timestamp reversal is dependent on the cockroach cluster and the application's usage patterns.

## When does a timestamp reversal matter?

Now we know when timestamps _could_ be reversed. But when does that matter to your application?

The TL;DR is: only when you care about the New Enemy Problem.

Let's take a look at a couple of examples of how reversed timestamps may be an issue for an application storing permissions in SpiceDB.

### Neglecting ACL Update Order

Two separate `WriteRelationship` calls come in:

`A`: Alice removes Bob from the `shared` folder
`B`: Alice adds a new document `not-for-bob.txt` to the `shared` folder

The normal case is that the timestamp for `A` < the timestamp for `B`.

But if those two writes hit the conditions for a timestamp reversal, then `B < A`.

From Alice's perspective, there should be no time at which Bob can ever see `not-for-bob.txt`.
She performed the first write, got a response, and then performed the second write.

But this isn't true when using `MinimizeLatency` or `AtLeastAsFresh` consistency.
If Bob later performs a `Check` request for the `not-for-bob.txt` document, it's possible that SpiceDB will pick an evaluation timestamp such that `B < T < A`, so that the document is in the folder _and_ bob is allowed to see the contents of the folder.

Note that this is only possible if `A - T < quantization window`: the check has to happen soon enough after the write for `A` that it's possible that SpiceDB picks a timestamp in between them.
The default quantization window is `5s`.

#### Application Mitigations for ACL Update Order

This could be mitigated in your application by:

- Not caring about the problem
- Not allowing the write from `B` within the max_offset time of the CRDB cluster (or the quantization window).
- Not allowing a Check on a resource within max_offset of its ACL modification (or the quantization window).

### Mis-apply Old ACLs to New Content

Two separate API calls come in:

`A`: Alice remove Bob as a viewer of document `secret`
`B`: Alice does a `FullyConsistent` `Check` request to get a ZedToken
`C`: Alice stores that ZedToken (timestamp `B`) with the document `secret` when she updates it to say `Bob is a fool`.

Same as before, the normal case is that the timestamp for `A` < the timestamp for `B`, but if the two writes hit the conditions for a timestamp reversal, then `B < A`.

Bob later tries to read the document. The application performs an `AtLeastAsFresh` `Check` for Bob to access the document `secret` using the stored Zedtoken (which is timestamp `B`.)

It's possible that SpiceDB will pick an evaluation timestamp `T` such that `B < T < A`, so that bob is allowed to read the newest contents of the document, and discover that Alice thinks he is a fool.

Same as before, this is only possible if `A - T < quantization window`: Bob's check has to happen soon enough after the write for `A` that it's possible that SpiceDB picks a timestamp in between `A` and `B`, and the default quantization window is `5s`.

#### Application Mitigations for Misapplying Old ACLs

This could be mitigated in your application by:

- Not caring about the problem
- Waiting for max_offset (or the quantization window) before doing the fully-consistent check.

## When does a timestamp reversal _not_ matter?

There are also some cases when there is no New Enemy Problem even if there are reversed timestamps.

### Non-sensitive domain

Not all authorization problems have a version of the New Enemy Problem, which relies on there being some meaningful
consequence of hitting an incorrect ACL during the small window of time where it's possible.

If the worst thing that happens from out-of-order ACL updates is that some users briefly see some non-sensitive data,
or that a user retains access to something that they already had access to for a few extra seconds, then even though
there could still effectively be a "New Enemy Problem," it's not a meaningful problem to worry about.

### Disjoint SpiceDB Graphs

The examples of the New Enemy Problem above rely on out-of-order ACLs to be part of the same permission graph.
But not all ACLs are part of the same graph, for example:

```haskell
definition user {}

definition blog {
    relation author: user
    permission edit = author
}

defintion video {
    relation editor: user
    permission change_tags = editor
}
```

`A`: Alice is added as an `author` of the Blog entry `new-enemy`
`B`: Bob is removed from the `editor`s of the `spicedb.mp4` video

If these writes are given reversed timestamps, it is possible that the ACLs will be applied out-or-order and this would
normally be a New Enemy Problem. But the ACLs themselves aren't shared between any permission computations, and so there
is no actual consequence to reversed timestamps.
