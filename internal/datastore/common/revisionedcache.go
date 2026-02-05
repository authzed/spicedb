package common

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	// quantizationWindowMultiplier is the multiplier applied to the quantization window
	// when determining which historical ranges to delete. Ranges older than
	// (quantizationWindow * multiplier) are eligible for deletion.
	quantizationWindowMultiplier = 2
)

// revisionRange represents a range of revisions for which a particular schema is valid.
// Ranges are stored in a linear list, ordered by start revision.
type revisionRange[R datastore.Revision] struct {
	startRevision      R                  // The first revision where this schema is valid (inclusive)
	checkpointRevision R                  // The latest revision confirmed valid by the watcher
	isOpenEnded        bool               // Whether this range extends indefinitely (current schema)
	schema             *core.StoredSchema // The cached schema
	schemaHash         string             // Hash of the schema for quick comparison
}

// contains checks if the given revision falls within this range.
// The range is only valid from startRevision up to and including checkpointRevision.
// This is safe because we only trust revisions that have been confirmed by the watcher.
func (r *revisionRange[R]) contains(rev R) bool {
	// Check if revision is >= startRevision
	if rev.LessThan(r.startRevision) {
		return false
	}

	// For both open-ended and historical ranges, we only trust up to the checkpoint
	// This is safe: we won't return cached data for revisions we haven't confirmed
	// Only valid if revision <= checkpoint (checkpoint is inclusive)
	return rev.LessThan(r.checkpointRevision) || rev.Equal(r.checkpointRevision)
}

// SchemaLoader is a function that loads a schema from the datastore at the current revision.
// It should return the schema and the revision at which it was loaded.
type SchemaLoader[R datastore.Revision] func(ctx context.Context) (*core.StoredSchema, R, error)

// cacheUpdate represents a pending cache update operation
type cacheUpdate[R datastore.Revision] struct {
	revision   R
	schema     *core.StoredSchema
	schemaHash string
	checkpoint R
}

// cacheSnapshot is an immutable snapshot of the cache state
type cacheSnapshot[R datastore.Revision] struct {
	currentRange     *revisionRange[R]
	historicalRanges []*revisionRange[R]
	totalSize        uint64
}

// RevisionedSchemaCache provides a lock-free revision-aware cache for schemas using atomics.
// It maintains historical closed ranges and a single current open range.
// The cache automatically warms on startup and updates ranges as schema changes are detected.
type RevisionedSchemaCache[R datastore.Revision] struct {
	// snapshot is atomically swapped for lock-free reads
	snapshot atomic.Pointer[cacheSnapshot[R]]

	// options contains cache configuration
	options options.SchemaCacheOptions

	// watcher is used to detect schema changes
	watcher datastore.SingleStoreSchemaHashWatcher

	// schemaLoader is called to load schemas when changes are detected or for warming
	schemaLoader SchemaLoader[R]

	// watchCtx and watchCancel manage the schema watch lifecycle
	watchCtx    context.Context
	watchCancel context.CancelFunc

	// updateChan is used to send async cache updates (size 1 for latest-wins behavior)
	updateChan chan cacheUpdate[R]

	// updateDone signals when the update goroutine has stopped
	updateDone chan struct{}

	// flushChan is used to request a flush and receive confirmation
	flushChan chan chan struct{}
}

// NewRevisionedSchemaCache creates a new lock-free revision-aware schema cache.
// Call StartWatching() after creation to begin monitoring for schema changes.
func NewRevisionedSchemaCache[R datastore.Revision](
	cacheOptions options.SchemaCacheOptions,
) *RevisionedSchemaCache[R] {
	cache := &RevisionedSchemaCache[R]{
		options:    cacheOptions,
		updateChan: make(chan cacheUpdate[R], 1), // Buffer of 1 for latest-wins behavior
		updateDone: make(chan struct{}),
		flushChan:  make(chan chan struct{}),
	}

	// Initialize with empty snapshot
	cache.snapshot.Store(&cacheSnapshot[R]{
		historicalRanges: make([]*revisionRange[R], 0),
	})

	// Start the background update processor
	go cache.processUpdates()

	return cache
}

// WarmCache loads the current schema into the cache on startup.
// This should be called after StartWatching to pre-populate the cache.
func (c *RevisionedSchemaCache[R]) WarmCache(ctx context.Context) error {
	if c.schemaLoader == nil {
		return nil
	}

	schema, revision, err := c.schemaLoader(ctx)
	if err != nil {
		return fmt.Errorf("failed to warm cache: %w", err)
	}

	if schema == nil {
		return nil
	}

	// Extract hash from the loaded schema
	schemaHash := ""
	if v1 := schema.GetV1(); v1 != nil {
		schemaHash = v1.SchemaHash
	}

	// Add as the initial open-ended range with checkpoint set to the current revision
	if err := c.setWithCheckpoint(revision, schema, schemaHash, revision); err != nil {
		return fmt.Errorf("failed to set initial schema in cache: %w", err)
	}

	log.Info().
		Str("schemaHash", schemaHash).
		Str("revision", revision.String()).
		Msg("warmed schema cache with current schema")

	return nil
}

// StartWatching begins monitoring for schema hash changes using the provided watcher and schema loader.
// The schema loader is called to pre-fill the cache when schema changes are detected.
// This should be called after the datastore is fully initialized.
func (c *RevisionedSchemaCache[R]) StartWatching(watcher datastore.SingleStoreSchemaHashWatcher, schemaLoader SchemaLoader[R]) {
	if watcher == nil {
		return
	}

	if c.watchCancel != nil {
		// Already watching
		return
	}

	c.watcher = watcher
	c.schemaLoader = schemaLoader
	c.watchCtx, c.watchCancel = context.WithCancel(context.Background())
	go c.watchSchemaChanges()
}

// Close stops the schema watcher and cleans up resources.
func (c *RevisionedSchemaCache[R]) Close() {
	if c.watchCancel != nil {
		c.watchCancel()
	}

	// Close the update channel and wait for processor to finish
	close(c.updateChan)
	<-c.updateDone
}

// Flush waits for all pending async updates to complete.
// This is primarily useful for testing to ensure updates have been processed.
func (c *RevisionedSchemaCache[R]) Flush() {
	// Check if cache is already closed
	select {
	case <-c.updateDone:
		return
	default:
	}

	// Send flush request and wait for response
	responseChan := make(chan struct{})
	c.flushChan <- responseChan
	<-responseChan
}

// watchSchemaChanges monitors for schema hash changes and updates the cache accordingly.
func (c *RevisionedSchemaCache[R]) watchSchemaChanges() {
	callback := func(schemaHash string, revision datastore.Revision) error {
		// Type assert the revision to our specific type
		typedRevision, ok := revision.(R)
		if !ok {
			return fmt.Errorf("revision type mismatch: expected %T, got %T", *new(R), revision)
		}

		snap := c.snapshot.Load()

		if snap.currentRange != nil {
			if snap.currentRange.schemaHash != schemaHash {
				// Schema changed - need to close current range and create new one
				log.Info().
					Str("oldHash", snap.currentRange.schemaHash).
					Str("newHash", schemaHash).
					Str("closedAtRevision", typedRevision.String()).
					Msg("schema hash changed, closing existing range")

				// Load the new schema immediately to pre-fill the cache
				if c.schemaLoader != nil {
					loadCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					schema, loadedAtRevision, loadErr := c.schemaLoader(loadCtx)
					cancel()

					if loadErr != nil {
						log.Warn().Err(loadErr).Msg("failed to pre-load new schema after hash change")
					} else if schema != nil {
						// Extract hash from the loaded schema
						loadedHash := ""
						if v1 := schema.GetV1(); v1 != nil {
							loadedHash = v1.SchemaHash
						}

						// Send update to close old range and create new one
						// The setWithCheckpoint will handle the transition
						update := cacheUpdate[R]{
							revision:   loadedAtRevision,
							schema:     schema,
							schemaHash: loadedHash,
							checkpoint: loadedAtRevision,
						}

						select {
						case c.updateChan <- update:
							log.Info().
								Str("schemaHash", loadedHash).
								Str("startRevision", loadedAtRevision.String()).
								Msg("sent pre-loaded new schema to cache")
						default:
							// Channel full, will be loaded on demand
						}
					}
				}
			} else {
				// Schema hash unchanged - update checkpoint to this revision
				// Send an update with the same hash but new checkpoint
				update := cacheUpdate[R]{
					revision:   snap.currentRange.startRevision, // Keep same start
					schema:     snap.currentRange.schema,        // Keep same schema
					schemaHash: schemaHash,                      // Same hash
					checkpoint: typedRevision,                   // Update checkpoint
				}

				select {
				case c.updateChan <- update:
					log.Debug().
						Str("schemaHash", schemaHash).
						Str("checkpointRevision", typedRevision.String()).
						Msg("sent checkpoint update for current schema")
				default:
					// Channel full, older update will be used
				}
			}
		}

		return nil
	}

	// Calculate refresh interval: quantization window / 2, with a reasonable default
	refreshInterval := 5 * time.Second
	if c.options.QuantizationWindow > 0 {
		refreshInterval = c.options.QuantizationWindow / 2
		// Ensure a minimum refresh interval
		if refreshInterval < 1*time.Second {
			refreshInterval = 1 * time.Second
		}
	}

	if err := c.watcher.WatchSchemaHash(c.watchCtx, refreshInterval, callback); err != nil {
		// Only log if it's not a context cancellation (normal shutdown)
		if c.watchCtx.Err() == nil {
			log.Error().Err(err).Msg("schema hash watcher failed")
		}
	}
}

// Get retrieves a schema for the given revision from the cache.
// Returns nil if the schema is not cached for that revision.
// This is a lock-free read using atomic.Load.
func (c *RevisionedSchemaCache[R]) Get(revision R) *core.StoredSchema {
	// Atomic load of the current snapshot (lock-free!)
	snap := c.snapshot.Load()

	// If there's no current range, the cache is empty
	if snap.currentRange == nil {
		return nil
	}

	// Check current range first (most common case)
	if snap.currentRange.contains(revision) {
		return snap.currentRange.schema
	}

	// Check historical ranges (search backwards as more recent is more likely)
	for i := len(snap.historicalRanges) - 1; i >= 0; i-- {
		if snap.historicalRanges[i].contains(revision) {
			return snap.historicalRanges[i].schema
		}
	}

	return nil
}

// Set stores a schema in the cache for a specific revision asynchronously.
// This creates a new open-ended range starting at the given revision with checkpoint set to the start revision.
// The operation is performed in the background to avoid blocking the read path.
func (c *RevisionedSchemaCache[R]) Set(revision R, schema *core.StoredSchema, schemaHash string) error {
	// Always set checkpoint to at least the start revision
	update := cacheUpdate[R]{
		revision:   revision,
		schema:     schema,
		schemaHash: schemaHash,
		checkpoint: revision,
	}

	select {
	case c.updateChan <- update:
		// Sent successfully
	default:
		// Channel full, drop the update
	}

	return nil
}

// processUpdates processes cache updates in the background.
// Creates new immutable snapshots for each update.
func (c *RevisionedSchemaCache[R]) processUpdates() {
	defer close(c.updateDone)

	for {
		select {
		case update, ok := <-c.updateChan:
			if !ok {
				return
			}
			c.applyUpdate(update)

		case responseChan := <-c.flushChan:
			// Drain any pending updates before responding to flush
			for {
				select {
				case update, ok := <-c.updateChan:
					if !ok {
						close(responseChan)
						return
					}
					c.applyUpdate(update)
				default:
					// No more pending updates
					close(responseChan)
					goto continueLoop
				}
			}
		continueLoop:
		}
	}
}

// applyUpdate applies an update by creating a new snapshot
func (c *RevisionedSchemaCache[R]) applyUpdate(update cacheUpdate[R]) {
	// Load current snapshot
	oldSnap := c.snapshot.Load()

	// Validate schema exists
	if update.schema == nil {
		log.Warn().Msg("cannot cache nil schema")
		return
	}

	// Check if update is needed
	if oldSnap.currentRange != nil {
		if update.revision.LessThan(oldSnap.currentRange.startRevision) {
			return // Reading from the past
		}

		// If same hash, update checkpoint if newer
		if oldSnap.currentRange.schemaHash == update.schemaHash {
			if !update.checkpoint.GreaterThan(oldSnap.currentRange.checkpointRevision) {
				return // Checkpoint not newer
			}

			// Create new snapshot with updated checkpoint
			newCurrentRange := &revisionRange[R]{
				startRevision:      oldSnap.currentRange.startRevision,
				checkpointRevision: update.checkpoint,
				isOpenEnded:        oldSnap.currentRange.isOpenEnded,
				schema:             oldSnap.currentRange.schema,
				schemaHash:         oldSnap.currentRange.schemaHash,
			}

			newSnap := &cacheSnapshot[R]{
				currentRange:     newCurrentRange,
				historicalRanges: oldSnap.historicalRanges, // Reuse slice (immutable)
				totalSize:        oldSnap.totalSize,
			}

			c.snapshot.Store(newSnap)

			log.Debug().
				Str("schemaHash", update.schemaHash).
				Str("updatedCheckpoint", update.checkpoint.String()).
				Msg("updated checkpoint for current range")
			return
		}
	}

	// Different hash - need to create new range
	schemaSize := uint64(update.schema.SizeVT())

	// Create new historical ranges slice (pre-allocate for current + existing historical)
	estimatedSize := len(oldSnap.historicalRanges)
	if oldSnap.currentRange != nil {
		estimatedSize++
	}
	newHistorical := make([]*revisionRange[R], 0, estimatedSize)
	newTotalSize := schemaSize

	// Add old current range to historical if it exists
	if oldSnap.currentRange != nil {
		// Close the old range - keep its checkpoint as-is (no longer open-ended)
		// SAFETY: We only trust this range up to its checkpoint, not to the new revision
		// There could have been schema changes between the checkpoint and the new revision
		// that we didn't detect
		closedRange := &revisionRange[R]{
			startRevision:      oldSnap.currentRange.startRevision,
			checkpointRevision: oldSnap.currentRange.checkpointRevision,
			isOpenEnded:        false,
			schema:             oldSnap.currentRange.schema,
			schemaHash:         oldSnap.currentRange.schemaHash,
		}
		newHistorical = append(newHistorical, closedRange)
		newTotalSize += uint64(oldSnap.currentRange.schema.SizeVT())
	}

	// Copy existing historical ranges
	for _, rng := range oldSnap.historicalRanges {
		newHistorical = append(newHistorical, rng)
		newTotalSize += uint64(rng.schema.SizeVT())
	}

	// Apply memory eviction if needed
	if c.options.MaximumCacheMemoryBytes > 0 && newTotalSize > c.options.MaximumCacheMemoryBytes {
		newHistorical, newTotalSize = c.evictOldest(newHistorical, newTotalSize, schemaSize)
	}

	// Apply time-based cleanup if needed
	if c.options.QuantizationWindow > 0 {
		newHistorical, newTotalSize = c.cleanupOldRanges(newHistorical, newTotalSize, update.revision)
	}

	// Create new current range
	newCurrentRange := &revisionRange[R]{
		startRevision:      update.revision,
		checkpointRevision: update.checkpoint,
		isOpenEnded:        true,
		schema:             update.schema,
		schemaHash:         update.schemaHash,
	}

	// Create and store new snapshot
	newSnap := &cacheSnapshot[R]{
		currentRange:     newCurrentRange,
		historicalRanges: newHistorical,
		totalSize:        newTotalSize,
	}

	c.snapshot.Store(newSnap)

	log.Debug().
		Str("schemaHash", update.schemaHash).
		Str("startRevision", update.revision.String()).
		Str("checkpoint", update.checkpoint.String()).
		Msg("set current schema range")
}

// evictOldest removes the oldest historical ranges until we have enough space
func (c *RevisionedSchemaCache[R]) evictOldest(
	ranges []*revisionRange[R],
	currentSize uint64,
	neededSpace uint64,
) ([]*revisionRange[R], uint64) {
	if currentSize+neededSpace <= c.options.MaximumCacheMemoryBytes {
		return ranges, currentSize
	}

	spaceToFree := (currentSize + neededSpace) - c.options.MaximumCacheMemoryBytes
	newSize := currentSize

	// Keep removing oldest (index 0) until we have space or hit minimum
	var keptRanges []*revisionRange[R]
	freedSpace := uint64(0)

	for i, rng := range ranges {
		// Always keep at least 1 historical range (minimum 2 total with current)
		if len(ranges)-i <= 1 {
			keptRanges = append(keptRanges, ranges[i:]...)
			break
		}

		rangeSize := uint64(rng.schema.SizeVT())
		if freedSpace < spaceToFree {
			freedSpace += rangeSize
			newSize -= rangeSize
		} else {
			keptRanges = append(keptRanges, rng)
		}
	}

	return keptRanges, newSize
}

// cleanupOldRanges removes historical ranges older than the quantization window
func (c *RevisionedSchemaCache[R]) cleanupOldRanges(
	ranges []*revisionRange[R],
	currentSize uint64,
	currentRevision R,
) ([]*revisionRange[R], uint64) {
	currentWithTimestamp, hasTimestamp := any(currentRevision).(revisions.WithTimestampRevision)
	if !hasTimestamp {
		return ranges, currentSize
	}

	currentTime := currentWithTimestamp.TimestampNanoSec()
	cutoffTime := currentTime - (c.options.QuantizationWindow.Nanoseconds() * quantizationWindowMultiplier)

	var keptRanges []*revisionRange[R]
	newSize := currentSize

	for _, rng := range ranges {
		// Use checkpoint revision to determine age - this is the latest time we confirmed this schema
		checkpointWithTimestamp, ok := any(rng.checkpointRevision).(revisions.WithTimestampRevision)
		if ok && checkpointWithTimestamp.TimestampNanoSec() < cutoffTime {
			// Too old, evict it
			newSize -= uint64(rng.schema.SizeVT())
		} else {
			keptRanges = append(keptRanges, rng)
		}
	}

	return keptRanges, newSize
}

// setWithCheckpoint is a helper to send updates with checkpoint via the async channel
func (c *RevisionedSchemaCache[R]) setWithCheckpoint(revision R, schema *core.StoredSchema, schemaHash string, checkpoint R) error {
	update := cacheUpdate[R]{
		revision:   revision,
		schema:     schema,
		schemaHash: schemaHash,
		checkpoint: checkpoint,
	}

	select {
	case c.updateChan <- update:
		// Sent successfully
	default:
		// Channel full, drop the update
	}

	return nil
}
