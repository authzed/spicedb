## 2.2.1 - 2025-07-22

### 🚀 Improvements

- Added more detailed explanations of the mechanics related to returning `ErrNotFound` ([#136](https://github.com/maypok86/otter/issues/136))

### 🐞 Bug Fixes

- Fix inconsistent singleflight results if the key is invalidated on the way ([#137](https://github.com/maypok86/otter/issues/137))
- Fix panic during concurrent execution of `InvalidateAll` and `Get` under high contention ([#139](https://github.com/maypok86/otter/issues/139))

## 2.2.0 - 2025-07-07

This release focuses on improving the integration experience with pull-based metric collectors.

### ✨Features

- Added `IsWeighted`, `IsRecordingStats` and `Stats` methods for cache ([#131](https://github.com/maypok86/otter/issues/131))
- Added `Minus` and `Plus` methods for `stats.Stats`
- Added `stats.Snapshoter` and `stats.SnapshotRecorder` interfaces

### 🚀 Improvements

- Reduced memory consumption of `stats.Counter` by 4 times

## 2.1.1 - 2025-07-05

### 🚀 Improvements

- `Get` now returns the value from the loader even when an error is returned. ([#132](https://github.com/maypok86/otter/issues/132))

## 2.1.0 - 2025-06-29

### ✨Features

- Added `Compute`, `ComputeIfAbsent` and `ComputeIfPresent` methods
- Added `LoadCacheFrom`, `LoadCacheFromFile`, `SaveCacheTo` and `SaveCacheToFile` functions
- Added `Clock` interface and option for time mocking
- Added `Keys` and `Values` iterators
- Added `Hottest` and `Coldest` iterators

### 🚀 Improvements

- Slightly reduced memory consumption
- Cache became significantly faster in cases when it's lightly populated
- Reduced number of allocations during refresh

### 🐞 Bug Fixes

- Fixed a bug in timer wheel ([#64](https://github.com/Yiling-J/theine-go/issues/64))
- Added usage of `context.WithoutCancel` during refresh execution ([#124](https://github.com/maypok86/otter/issues/124))

## 2.0.0 - 2025-06-18

### 📝 Description

Otter v2 has been completely redesigned for better performance and usability.

Key improvements:
- Completely rethought API for greater flexibility
- Added [loading](https://maypok86.github.io/otter/user-guide/v2/features/loading/) and [refreshing](https://maypok86.github.io/otter/user-guide/v2/features/refresh/) features ([#26](https://github.com/maypok86/otter/issues/26))
- Added [entry pinning](https://maypok86.github.io/otter/user-guide/v2/features/eviction/#pinning-entries)
- Replaced eviction policy with adaptive W-TinyLFU, enabling Otter to achieve one of the highest hit rates across **all** workloads.
- Added HashDoS protection against potential attacks
- The task scheduling mechanism has been completely reworked, allowing users to manage it themselves when needed
- Added more efficient write buffer
- Added auto-configurable lossy read buffer
- Optimized hash table
- Test coverage increased to 97%

### 🚨 Breaking Changes

1. **Cache Creation**
   - Removed `Builder` pattern in favor of canonical `Options` struct
   - `MustBuilder` and `Builder` methods are replaced with `Must` and `New` functions
   - `Cost` renamed to `Weight`
   - Replaced unified `capacity` with explicit `MaximumSize` and `MaximumWeight` parameters
   - Replaced `DeletionListener` with `OnDeletion` and `OnAtomicDeletion` handlers
   - The ability to create a cache with any combination of features

2. **Cache API Changes**
    - `Get` method renamed to `GetIfPresent`
    - `Set` method signature changed to return both value and bool
    - `SetIfAbsent` method signature changed to return both value and bool
    - `Delete` method renamed to `Invalidate`
    - `Clear` method renamed to `InvalidateAll`
    - `Size` method renamed to `EstimatedSize`
    - `Capacity` method renamed to `GetMaximum`
    - `Range` method removed in favor of `All` iterator
    - `Has` method removed
    - `DeleteByFunc` method removed
    - `Stats` method removed in favor of `stats.Recorder` interface
    - `Close` method removed

3. **Expiration**
    - Expiration API is now more flexible with `ExpiryCalculator` interface
    - `ExpiryCreating`, `ExpiryWriting` and `ExpiryAccessing` functions introduced

4. **Statistics**
    - Moved statistics to a separate package `stats`
    - `stats.Recorder` interface and `stats.Counter` struct introduced

5. **Extension**
    - `Extension` struct removed in favor of methods from `Cache`

### ✨Features

1. **Loading**
    - Added `Get` method for obtaining values if necessary
    - Added `Loader` interface for retrieving values from the data source
    - Added `ErrNotFound` error for indicating missing entries

2. **Refresh**
    - Added flexible refresh API with `RefreshCalculator` interface
   - `RefreshCreating`, `RefreshWriting` functions introduced
   - Added `Refresh` method for refreshing values asynchronously

3. **Bulk Operations**
    - Added `BulkGet` for loading multiple values at once
    - Added `BulkRefresh` for refreshing multiple values asynchronously
    - Added `BulkLoader` interface for retrieving multiple values from the data source at once

4. **Cache Methods**
    - Added `All` method for iterating over all entries
    - Added `CleanUp` method for performing pending maintenance operations
    - Added `WeightedSize` method for weight-based caches

5. **Enhanced Configuration**
    - Added `Executor` option for customizing async operations
    - Added `Logger` interface for custom logging

6. **Entry Management**
   - Added `SetExpiresAfter` and `SetRefreshableAfter` for per-entry time control
   - Added `GetEntry` and `GetEntryQuietly` methods for accessing cache entries
   - Most `Entry`'s methods replaced with public fields for direct access.

7. **Deletion Notifications**
   - Replaced `DeletionListener` with `OnDeletion` and `OnAtomicDeletion` handlers
   - Deletion causes renamed for clarity and consistency.
   - Added `IsEviction` method
   - Added `DeletionEvent` struct

8. **Performance Improvements**
    - Replaced `S3-FIFO` with adaptive `W-TinyLFU`
    - Added more efficient write buffer
    - Added auto-configurable lossy read buffer
    - The task scheduling mechanism has been completely reworked, allowing users to manage it themselves when needed.

### 🚀 Improvements

- Added [loading](https://maypok86.github.io/otter/user-guide/v2/features/loading/) and [refreshing](https://maypok86.github.io/otter/user-guide/v2/features/refresh/) features ([#26](https://github.com/maypok86/otter/issues/26))
- You can now pass a custom implementation of the `stats.Recorder` interface ([#119](https://github.com/maypok86/otter/issues/119))
- You can now use a TTL shorter than `time.Second` ([#115](https://github.com/maypok86/otter/issues/115))

## 1.2.4 - 2024-11-23

### 🐞 Bug Fixes

- Fixed a bug due to changing [gammazero/deque](https://github.com/gammazero/deque/pull/33) contracts without v2 release. ([#112](https://github.com/maypok86/otter/issues/112))

## 1.2.3 - 2024-09-30

### 🐞 Bug Fixes

- Added collection of eviction statistics for expired entries. ([#108](https://github.com/maypok86/otter/issues/108))

## 1.2.2 - 2024-08-14

### ✨️Features

- Implemented `fmt.Stringer` interface for `DeletionReason` type ([#100](https://github.com/maypok86/otter/issues/100))

### 🐞 Bug Fixes

- Fixed processing of an expired entry in the `Get` method ([#98](https://github.com/maypok86/otter/issues/98))
- Fixed inconsistent deletion listener behavior ([#98](https://github.com/maypok86/otter/issues/98))
- Fixed the behavior of `checkedAdd` when over/underflow ([#91](https://github.com/maypok86/otter/issues/91))

## 1.2.1 - 2024-04-15

### 🐞 Bug Fixes

- Fixed uint32 capacity overflow.

## 1.2.0 - 2024-03-12

The main innovation of this release is the addition of an `Extension`, which makes it easy to add a huge number of features to otter.

Usage example:

```go
key := 1
...
entry, ok := cache.Extension().GetEntry(key)
...
key := entry.Key()
value := entry.Value()
cost := entry.Cost()
expiration := entry.Expiration()
ttl := entry.TTL()
hasExpired := entry.HasExpired()
```

### ✨️Features

- Added `DeletionListener` to the builder ([#63](https://github.com/maypok86/otter/issues/63))
- Added `Extension` ([#56](https://github.com/maypok86/otter/issues/56))

### 🚀 Improvements

- Added support for Go 1.22
- Memory consumption with small cache sizes is reduced to the level of other libraries ([#66](https://github.com/maypok86/otter/issues/66))

## 1.1.1 - 2024-03-06

### 🐞 Bug Fixes

- Fixed alignment issues on 32-bit archs

## 1.1.0 - 2024-03-04

The main innovation of this release is node code generation. Thanks to it, the cache will no longer consume more memory due to features that it does not use. For example, if you do not need an expiration policy, then otter will not store the expiration time of each entry. It also allows otter to use more effective expiration policies.

Another expected improvement is the correction of minor synchronization problems due to the state machine. Now otter, unlike other contention-free caches in Go, should not have them at all.

### ✨️Features

- Added `DeleteByFunc` function to cache ([#44](https://github.com/maypok86/otter/issues/44))
- Added `InitialCapacity` function to builder ([#47](https://github.com/maypok86/otter/issues/47))
- Added collection of additional statistics ([#57](https://github.com/maypok86/otter/issues/57))

### 🚀 Improvements

- Added proactive queue-based and timer wheel-based expiration policies with O(1) time complexity ([#55](https://github.com/maypok86/otter/issues/55))
- Added node code generation ([#55](https://github.com/maypok86/otter/issues/55))
- Fixed the race condition when changing the order of events ([#59](https://github.com/maypok86/otter/issues/59))
- Reduced memory consumption on small caches

## 1.0.0 - 2024-01-26

### ✨️Features

- Builder pattern support
- Cleaner API compared to other caches ([#40](https://github.com/maypok86/otter/issues/40))
- Added `SetIfAbsent` and `Range` functions ([#27](https://github.com/maypok86/otter/issues/27))
- Statistics collection ([#4](https://github.com/maypok86/otter/issues/4))
- Cost based eviction
- Support for generics and any comparable types as keys
- Support ttl ([#14](https://github.com/maypok86/otter/issues/14))
- Excellent speed ([benchmark results](https://github.com/maypok86/otter?tab=readme-ov-file#-performance-))
- O(1) worst case time complexity for S3-FIFO instead of O(n)
- Improved hit ratio of S3-FIFO on many traces ([simulator results](https://github.com/maypok86/otter?tab=readme-ov-file#-hit-ratio-))
