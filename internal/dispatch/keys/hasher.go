package keys

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type hashableValue interface {
	AppendToHash(hasher hasherInterface)
}

type hasherInterface interface {
	WriteString(value string)
	Write(p []byte)
}

type hashableRelationReference struct {
	*core.RelationReference
}

func (hrr hashableRelationReference) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(tuple.StringCoreRR(hrr.RelationReference))
}

type hashableResultSetting v1.DispatchCheckRequest_ResultsSetting

func (hrs hashableResultSetting) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(string(hrs))
}

type hashableIds []string

func (hid hashableIds) AppendToHash(hasher hasherInterface) {
	// Sort the IDs to canonicalize them. We have to clone to ensure that this does cause issues
	// with others accessing the slice.
	c := make([]string, len(hid))
	copy(c, hid)
	slices.Sort(c)

	for _, id := range c {
		hasher.WriteString(id)
		hasher.WriteString(",")
	}
}

type hashableOnr struct {
	*core.ObjectAndRelation
}

func (hnr hashableOnr) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(hnr.Namespace)
	hasher.WriteString(":")
	hasher.WriteString(hnr.ObjectId)
	hasher.WriteString("#")
	hasher.WriteString(hnr.Relation)
}

type hashableString string

func (hs hashableString) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(string(hs))
}

// hashableBytes feeds raw bytes into the cache-key hash. Used for Plan-Check
// keys, where the serialized iterator subtree is the dispatch identity —
// hashing the bytes directly avoids needing any parallel "canonical key"
// string alongside.
type hashableBytes []byte

func (hb hashableBytes) AppendToHash(hasher hasherInterface) {
	if len(hb) > 0 {
		hasher.Write(hb)
	}
}

type hashableLimit uint32

func (hl hashableLimit) AppendToHash(hasher hasherInterface) {
	if hl > 0 {
		hasher.WriteString(strconv.Itoa(int(hl)))
	}
}

type hashableCursor struct{ *v1.Cursor }

func (hc hashableCursor) AppendToHash(hasher hasherInterface) {
	if hc.Cursor != nil {
		for _, section := range hc.Sections {
			hasher.WriteString(section)
			hasher.WriteString(",")
		}
	}
}

type hashableCursorSections struct {
	sections []string
}

func (hc hashableCursorSections) AppendToHash(hasher hasherInterface) {
	for _, section := range hc.sections {
		hasher.WriteString(section)
		hasher.WriteString(",")
	}
}

type hashableContextString string

func (hc hashableContextString) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(string(hc))
}

type hashableContext struct {
	*structpb.Struct
}

func (hc hashableContext) AppendToHash(hasher hasherInterface) {
	if hc.Struct == nil {
		return
	}
	stable, err := caveats.StableContextStringForHashing(hc.Struct)
	if err != nil {
		return
	}
	hasher.WriteString(stable)
}

// dispatchCacheKeyHash computes a DispatchCheckKey for the given prefix and any hashable values.
func dispatchCacheKeyHash(prefix cachePrefix, atRevision string, args ...hashableValue) DispatchCacheKey {
	hasher := newDispatchCacheKeyHasher(prefix)

	for _, arg := range args {
		arg.AppendToHash(hasher)
		hasher.WriteString("@")
	}

	hasher.WriteString(atRevision)
	return hasher.BuildKey()
}

type dispatchCacheKeyHasher struct {
	stableHasher *xxhash.Digest
}

func newDispatchCacheKeyHasher(prefix cachePrefix) *dispatchCacheKeyHasher {
	h := &dispatchCacheKeyHasher{
		stableHasher: xxhash.New(),
	}

	prefixString := string(prefix)
	h.WriteString(prefixString)
	h.WriteString("/")
	return h
}

// WriteString writes a single string to the hasher.
func (h *dispatchCacheKeyHasher) WriteString(value string) {
	h.mustWriteString(value)
}

// Write feeds raw bytes into the underlying xxhash digest. Used by
// hashableBytes so callers (e.g., Plan-Check keys hashing the serialized
// iterator) don't have to round-trip through string conversion.
func (h *dispatchCacheKeyHasher) Write(p []byte) {
	// xxhash.Digest.Write never returns an error; mirror mustWriteString.
	_, err := h.stableHasher.Write(p)
	if err != nil {
		panic(fmt.Errorf("got an error from writing to the stable hasher: %w", err))
	}
}

func (h *dispatchCacheKeyHasher) mustWriteString(value string) {
	// NOTE: xxhash doesn't seem to ever return an error for WriteString, but we check it just
	// to be on the safe side.
	_, err := h.stableHasher.WriteString(value)
	if err != nil {
		panic(fmt.Errorf("got an error from writing to the stable hasher: %w", err))
	}
}

// BuildKey returns the constructed DispatchCheckKey.
func (h *dispatchCacheKeyHasher) BuildKey() DispatchCacheKey {
	return DispatchCacheKey(h.stableHasher.Sum64())
}
