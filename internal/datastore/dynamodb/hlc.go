package dynamodb

import (
	"sync"
	"time"

	"github.com/authzed/spicedb/internal/datastore/revisions"
)

type HybridLogicalClock struct {
	mu       sync.Mutex
	physical int64
	logical  uint32
}

func NewHybridLogicalClock() *HybridLogicalClock {
	return &HybridLogicalClock{
		physical: time.Now().UnixNano(),
		logical:  0,
	}
}

func (hlc *HybridLogicalClock) Now() revisions.HLCRevision {
	hlc.mu.Lock()
	defer hlc.mu.Unlock()

	currentPhysical := time.Now().UnixNano()

	if currentPhysical > hlc.physical {
		hlc.physical = currentPhysical
		hlc.logical = 0
	} else {
		hlc.logical++
	}

	return revisions.NewHLC(hlc.physical, hlc.logical)
}

func (hlc *HybridLogicalClock) Update(receivedTimestamp revisions.HLCRevision) revisions.HLCRevision {
	hlc.mu.Lock()
	defer hlc.mu.Unlock()

	currentPhysical := time.Now().UnixNano()

	maxPhysical := max(currentPhysical, receivedTimestamp.TimestampNanoSec())

	if maxPhysical > hlc.physical {
		hlc.physical = maxPhysical
		hlc.logical = 0
	} else {
		hlc.logical++
	}

	if hlc.physical == receivedTimestamp.TimestampNanoSec() {
		hlc.logical = max(hlc.logical, receivedTimestamp.LogicalClock()+1)
	}

	return revisions.NewHLC(hlc.physical, hlc.logical)
}
