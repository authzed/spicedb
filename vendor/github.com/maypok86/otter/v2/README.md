<p align="center">
  <img src="docs/assets/logo.png" width="40%" height="auto" >
  <h2 align="center">In-memory caching library</h2>
</p>

<p align="center">
<a href="https://pkg.go.dev/github.com/maypok86/otter/v2"><img src="https://pkg.go.dev/badge/github.com/maypok86/otter/v2.svg" alt="Go Reference"></a>
<img src="https://github.com/maypok86/otter/actions/workflows/test.yml/badge.svg" />
<a href="https://github.com/maypok86/otter/actions?query=branch%3Amain+workflow%3ATest" >
    <img src="https://gist.githubusercontent.com/maypok86/2aae2cd39836dc7c258df7ffec602d1c/raw/coverage.svg"/></a>
<a href="https://github.com/maypok86/otter/releases"><img alt="GitHub Release" src="https://img.shields.io/github/v/release/maypok86/otter"></a>
<a href="https://github.com/avelino/awesome-go"><img src="https://awesome.re/mentioned-badge.svg" alt="Mentioned in Awesome Go"></a>
</p>

Otter is designed to provide an excellent developer experience while maintaining high performance. It aims to address the shortcomings of its predecessors and incorporates design principles from high-performance libraries in other languages (such as [Caffeine](https://github.com/ben-manes/caffeine)).

## 📖 Contents

- [Features](#features)
- [Usage](#usage)
  - [Requirements](#requirements)
  - [Installation](#installation)
  - [Examples](#examples)
- [Performance](#performance)
  - [Throughput](#throughput)
  - [Hit ratio](#hit-ratio)
  - [Memory consumption](#memory-consumption)
- [Projects using Otter](#projects)
- [Related works](#related-works)
- [Contribute](#contribute)
- [License](#license)

## ✨ Features <a id="features" />

Performance-wise, Otter provides:

- [High hit rates](https://maypok86.github.io/otter/performance/hit-ratio/) across all workload types via [adaptive W-TinyLFU](https://dl.acm.org/citation.cfm?id=3274816)
- [Excellent throughput](https://maypok86.github.io/otter/performance/throughput/) under high contention on most workload types
- Among the lowest [memory overheads](https://maypok86.github.io/otter/performance/memory-consumption/) across all cache capacities
- Automatic data structures configuration based on contention/parallelism and workload patterns

Otter also provides a highly configurable caching API, enabling any combination of these optional features:

- Size-based [eviction](https://maypok86.github.io/otter/user-guide/v2/features/eviction/#size-based) when a maximum is exceeded
- Time-based [expiration](https://maypok86.github.io/otter/user-guide/v2/features/eviction/#time-based) of entries, measured since last access or last write
- [Automatic loading](https://maypok86.github.io/otter/user-guide/v2/features/loading/) of entries into the cache
- [Asynchronously refresh](https://maypok86.github.io/otter/user-guide/v2/features/refresh/) when the first stale request for an entry occurs
- [Writes propagated](https://maypok86.github.io/otter/user-guide/v2/features/compute/) to an external resource
-  Accumulation of cache access [statistics](https://maypok86.github.io/otter/user-guide/v2/features/statistics/)
- [Saving cache](https://maypok86.github.io/otter/user-guide/v2/features/persistence/) to a file and loading cache from a file

## 📚 Usage <a id="usage" />

For more details, see our [user's guide](https://maypok86.github.io/otter/user-guide/v2/getting-started/) and browse the [API docs](https://pkg.go.dev/github.com/maypok86/otter) for the latest release.

### 📋 Requirements <a id="requirements" />

Otter requires [Go](https://go.dev/) version [1.24](https://go.dev/doc/devel/release#go1.24.0) or above.

### 🛠️ Installation <a id="installation" />

#### With v1

```shell
go get -u github.com/maypok86/otter
```

#### With v2

```shell
go get -u github.com/maypok86/otter/v2
```

See the [release notes](https://github.com/maypok86/otter/releases) for details of the changes.

Note that otter only supports the two most recent minor versions of Go.

Otter follows semantic versioning for the documented public API on stable releases. `v2` is the latest stable major version.

### ✏️ Examples <a id="examples" />

Otter uses a plain `Options` struct for cache configuration. Check out [otter.Options](https://pkg.go.dev/github.com/maypok86/otter/v2#Options) for more details.

Note that all features are optional. You can create a cache that acts as a simple hash table wrapper, with near-zero memory overhead for unused features — thanks to [node code generation](https://github.com/maypok86/otter/blob/main/cmd/generator/main.go).

**API Usage Example**
```go
package main

import (
    "context"
    "time"

    "github.com/maypok86/otter/v2"
    "github.com/maypok86/otter/v2/stats"
)

func main() {
    ctx := context.Background()

    // Create statistics counter to track cache operations
    counter := stats.NewCounter()

    // Configure cache with:
    // - Capacity: 10,000 entries
    // - 1 second expiration after last access
    // - 500ms refresh interval after writes
    // - Stats collection enabled
    cache := otter.Must(&otter.Options[string, string]{
        MaximumSize:       10_000,
        ExpiryCalculator:  otter.ExpiryAccessing[string, string](time.Second),  // Reset timer on reads/writes
        RefreshCalculator: otter.RefreshWriting[string, string](500 * time.Millisecond),  // Refresh after writes
        StatsRecorder:     counter,  // Attach stats collector
    })

    // Phase 1: Test basic expiration
    // -----------------------------
    cache.Set("key", "value")  // Add initial value

    // Wait for expiration (1 second)
    time.Sleep(time.Second)

    // Verify entry expired
    if _, ok := cache.GetIfPresent("key"); ok {
        panic("key shouldn't be found")  // Should be expired
    }

    // Phase 2: Test cache stampede protection
    // --------------------------------------
    loader := func(ctx context.Context, key string) (string, error) {
        time.Sleep(200 * time.Millisecond)  // Simulate slow load
        return "value1", nil  // Return new value
    }

    // Concurrent Gets would deduplicate loader calls
    value, err := cache.Get(ctx, "key", otter.LoaderFunc[string, string](loader))
    if err != nil {
        panic(err)
    }
    if value != "value1" {
        panic("incorrect value")  // Should get newly loaded value
    }

    // Phase 3: Test background refresh
    // --------------------------------
    time.Sleep(500 * time.Millisecond)  // Wait until refresh needed

    // New loader that returns updated value
    loader = func(ctx context.Context, key string) (string, error) {
        time.Sleep(100 * time.Millisecond)  // Simulate refresh
        return "value2", nil  // Return refreshed value
    }

    // This triggers async refresh but returns current value
    value, err = cache.Get(ctx, "key", otter.LoaderFunc[string, string](loader))
    if err != nil {
        panic(err)
    }
    if value != "value1" {  // Should get old value while refreshing
        panic("loader shouldn't be called during Get")
    }

    // Wait for refresh to complete
    time.Sleep(110 * time.Millisecond)

    // Verify refreshed value
    v, ok := cache.GetIfPresent("key")
    if !ok {
        panic("key should be found")  // Should still be cached
    }
    if v != "value2" {  // Should now have refreshed value
        panic("refresh should be completed")
    }
}
```

You can find more usage examples [here](https://maypok86.github.io/otter/user-guide/v2/examples/).

## 📊 Performance <a id="performance" />

The benchmark code can be found [here](./benchmarks).

### 🚀 Throughput <a id="throughput" />

Throughput benchmarks are a Go port of the caffeine [benchmarks](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/jmh/java/com/github/benmanes/caffeine/cache/GetPutBenchmark.java). This microbenchmark compares the throughput of caches on a zipf distribution, which allows to show various inefficient places in implementations.

You can find results [here](https://maypok86.github.io/otter/performance/throughput/).

### 🎯 Hit ratio <a id="hit-ratio" />

The hit ratio simulator tests caches on various traces:
1. Synthetic (Zipf distribution)
2. Traditional (widely known and used in various projects and papers)

You can find results [here](https://maypok86.github.io/otter/performance/hit-ratio/).

### 💾 Memory consumption <a id="memory-consumption" />

This benchmark quantifies the additional memory consumption across varying cache capacities.

You can find results [here](https://maypok86.github.io/otter/performance/memory-consumption/).

## 🏗️ Projects using Otter <a id="projects" />

Below is a list of known projects that use Otter:

- [Grafana](https://github.com/grafana/grafana): The open and composable observability and data visualization platform.
- [Centrifugo](https://github.com/centrifugal/centrifugo): Scalable real-time messaging server in a language-agnostic way.
- [FrankenPHP](https://github.com/php/frankenphp): The modern PHP app server
- [Unkey](https://github.com/unkeyed/unkey): Open source API management platform

## 🗃 Related works <a id="related-works" />

Otter is based on the following papers:

- [BP-Wrapper: A Framework Making Any Replacement Algorithms (Almost) Lock Contention Free](https://www.researchgate.net/publication/220966845_BP-Wrapper_A_System_Framework_Making_Any_Replacement_Algorithms_Almost_Lock_Contention_Free)
- [TinyLFU: A Highly Efficient Cache Admission Policy](https://dl.acm.org/citation.cfm?id=3149371)
- [Adaptive Software Cache Management](https://dl.acm.org/citation.cfm?id=3274816)
- [Denial of Service via Algorithmic Complexity Attack](https://www.usenix.org/legacy/events/sec03/tech/full_papers/crosby/crosby.pdf)
- [Hashed and Hierarchical Timing Wheels](https://ieeexplore.ieee.org/document/650142)
- [A large scale analysis of hundreds of in-memory cache clusters at Twitter](https://www.usenix.org/system/files/osdi20-yang.pdf)

## 👏 Contribute <a id="contribute" />

Contributions are welcome as always, before submitting a new PR please make sure to open a new issue so community members can discuss it.
For more information please see [contribution guidelines](./CONTRIBUTING.md).

Additionally, you might find existing open issues which can help with improvements.

This project follows a standard [code of conduct](./CODE_OF_CONDUCT.md) so that you can understand what actions will and will not be tolerated.

## 📄 License <a id="license" />

This project is Apache 2.0 licensed, as found in the [LICENSE](./LICENSE).
