/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"bytes"
	"context"
	"hash/crc32"
	"math/rand"
	"sort"
	"sync"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

const (
	maxLocalReplicaDistance        = 5
	defaultMinEntriesForRandomPick = 1000
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type rangeMode int

const (
	rangeModeCoveringSplit rangeMode = iota
	rangeModePickRandom
)

type keyRangeCache struct {
	endpointCache channelEndpointCache

	mu     sync.Mutex
	ranges []*cachedRange // sorted by limit key
	groups map[uint64]*cachedGroup

	accessCounter               int64
	deterministicRandom         bool
	minEntriesForRandomPickHint int
}

type cachedTablet struct {
	tabletUID     uint64
	incarnation   []byte
	serverAddress string
	distance      uint32
	skip          bool
	role          sppb.Tablet_Role
	location      string

	endpoint channelEndpoint
}

type cachedGroup struct {
	groupUID uint64

	mu         sync.Mutex
	generation []byte
	tablets    []*cachedTablet
	leaderIdx  int

	refs int32
}

type cachedRange struct {
	startKey   []byte
	limitKey   []byte
	group      *cachedGroup
	splitID    uint64
	generation []byte
	lastAccess int64
}

func newKeyRangeCache(endpointCache channelEndpointCache) *keyRangeCache {
	if endpointCache == nil {
		endpointCache = newPassthroughChannelEndpointCache()
	}
	return &keyRangeCache{
		endpointCache:               endpointCache,
		groups:                      make(map[uint64]*cachedGroup),
		minEntriesForRandomPickHint: defaultMinEntriesForRandomPick,
	}
}

func (c *keyRangeCache) useDeterministicRandom() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deterministicRandom = true
}

func (c *keyRangeCache) setMinEntriesForRandomPick(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if value <= 0 {
		value = defaultMinEntriesForRandomPick
	}
	c.minEntriesForRandomPickHint = value
}

func (c *keyRangeCache) addRanges(cacheUpdate *sppb.CacheUpdate) {
	if cacheUpdate == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	newGroups := make([]*cachedGroup, 0, len(cacheUpdate.GetGroup()))
	for _, groupIn := range cacheUpdate.GetGroup() {
		newGroups = append(newGroups, c.findOrInsertGroup(groupIn))
	}
	for _, rangeIn := range cacheUpdate.GetRange() {
		c.replaceRangeIfNewer(rangeIn)
	}
	for _, group := range newGroups {
		c.unrefGroup(group)
	}
}

func (c *keyRangeCache) fillRoutingHint(ctx context.Context, preferLeader bool, mode rangeMode, directedReadOptions *sppb.DirectedReadOptions, hint *sppb.RoutingHint) channelEndpoint {
	if hint == nil || len(hint.GetKey()) == 0 {
		return nil
	}
	if directedReadOptions == nil {
		directedReadOptions = &sppb.DirectedReadOptions{}
	}

	c.mu.Lock()
	targetRange := c.findRangeLocked(hint.GetKey(), hint.GetLimitKey(), mode)
	c.mu.Unlock()
	if targetRange == nil || targetRange.group == nil {
		return nil
	}

	hint.GroupUid = targetRange.group.groupUID
	hint.SplitId = targetRange.splitID
	hint.Key = append(hint.Key[:0], targetRange.startKey...)
	hint.LimitKey = append(hint.LimitKey[:0], targetRange.limitKey...)

	return targetRange.group.fillRoutingHint(ctx, c.endpointCache, preferLeader, directedReadOptions, hint)
}

func (c *keyRangeCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearLocked()
}

func (c *keyRangeCache) clearLocked() {
	c.ranges = nil
	c.groups = make(map[uint64]*cachedGroup)
	c.accessCounter = 0
}

func (c *keyRangeCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ranges)
}

func (c *keyRangeCache) shrinkTo(newSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if newSize <= 0 {
		c.clearLocked()
		return
	}
	if newSize >= len(c.ranges) {
		return
	}

	numToShrink := len(c.ranges) - newSize
	numToSample := numToShrink * 2
	if numToSample > len(c.ranges) {
		numToSample = len(c.ranges)
	}

	perm := rand.Perm(len(c.ranges))
	sampled := make([]*cachedRange, 0, numToSample)
	for i := 0; i < numToSample; i++ {
		sampled = append(sampled, c.ranges[perm[i]])
	}
	sort.Slice(sampled, func(i, j int) bool {
		return sampled[i].lastAccess < sampled[j].lastAccess
	})

	evicted := make(map[*cachedRange]struct{}, numToShrink)
	for i := 0; i < numToShrink; i++ {
		evicted[sampled[i]] = struct{}{}
	}

	kept := make([]*cachedRange, 0, len(c.ranges)-numToShrink)
	for _, r := range c.ranges {
		if _, ok := evicted[r]; ok {
			c.unrefGroup(r.group)
			continue
		}
		kept = append(kept, r)
	}
	c.ranges = kept
}

func (c *keyRangeCache) accessTimeNowLocked() int64 {
	c.accessCounter++
	return c.accessCounter
}

func (c *keyRangeCache) findRangeLocked(key, limit []byte, mode rangeMode) *cachedRange {
	idx := sort.Search(len(c.ranges), func(i int) bool {
		return bytes.Compare(c.ranges[i].limitKey, key) > 0
	})
	if idx >= len(c.ranges) {
		return nil
	}
	first := c.ranges[idx]
	startInRange := bytes.Compare(key, first.startKey) >= 0
	if len(limit) == 0 {
		if startInRange {
			first.lastAccess = c.accessTimeNowLocked()
			return first
		}
		return nil
	}
	if startInRange && bytes.Compare(limit, first.limitKey) <= 0 {
		first.lastAccess = c.accessTimeNowLocked()
		return first
	}
	if mode == rangeModeCoveringSplit {
		return nil
	}

	total := 0
	foundGap := !startInRange
	sampledIdx := idx
	lastLimit := first.startKey
	hitEnd := false

	i := idx
	for ; i < len(c.ranges); i++ {
		current := c.ranges[i]
		if bytes.Compare(lastLimit, current.startKey) != 0 {
			foundGap = true
			if bytes.Compare(current.startKey, limit) >= 0 {
				break
			}
		}
		total++
		if c.uniformRandomLocked(total, key, limit, current.startKey) == 0 {
			sampledIdx = i
		}
		lastLimit = current.limitKey
		if bytes.Compare(lastLimit, limit) >= 0 || total >= c.minEntriesForRandomPickHint {
			break
		}
	}
	if i >= len(c.ranges) {
		hitEnd = true
	}
	if hitEnd {
		foundGap = true
	}
	if !foundGap || total >= c.minEntriesForRandomPickHint {
		selected := c.ranges[sampledIdx]
		selected.lastAccess = c.accessTimeNowLocked()
		return selected
	}
	return nil
}

func (c *keyRangeCache) uniformRandomLocked(n int, seed1, seed2, seed3 []byte) int {
	if n <= 1 {
		return 0
	}
	if c.deterministicRandom {
		data := make([]byte, 0, len(seed1)+len(seed2)+len(seed3))
		data = append(data, seed1...)
		data = append(data, seed2...)
		data = append(data, seed3...)
		return int(crc32.Checksum(data, crc32cTable) % uint32(n))
	}
	return rand.Intn(n)
}

func (c *keyRangeCache) replaceRangeIfNewer(rangeIn *sppb.Range) {
	if rangeIn == nil {
		return
	}
	startKey := append([]byte(nil), rangeIn.GetStartKey()...)
	limitKey := append([]byte(nil), rangeIn.GetLimitKey()...)

	overlapIdx := make([]int, 0)
	for i, existing := range c.ranges {
		if bytes.Compare(existing.limitKey, startKey) <= 0 {
			continue
		}
		if bytes.Compare(existing.startKey, limitKey) >= 0 {
			continue
		}
		overlapIdx = append(overlapIdx, i)
	}

	if len(overlapIdx) == 0 {
		c.insertRangeLocked(&cachedRange{
			startKey:   startKey,
			limitKey:   limitKey,
			group:      c.findAndRefGroup(rangeIn.GetGroupUid()),
			splitID:    rangeIn.GetSplitId(),
			generation: append([]byte(nil), rangeIn.GetGeneration()...),
			lastAccess: c.accessTimeNowLocked(),
		})
		return
	}

	overlapping := make([]*cachedRange, 0, len(overlapIdx))
	for _, idx := range overlapIdx {
		existing := c.ranges[idx]
		cmp := bytes.Compare(rangeIn.GetGeneration(), existing.generation)
		if cmp < 0 || (cmp == 0 && bytes.Equal(existing.startKey, startKey) && bytes.Equal(existing.limitKey, limitKey)) {
			return
		}
		overlapping = append(overlapping, existing)
	}

	remove := make(map[int]struct{}, len(overlapIdx))
	for _, idx := range overlapIdx {
		remove[idx] = struct{}{}
	}
	remaining := make([]*cachedRange, 0, len(c.ranges)-len(overlapIdx)+3)
	for idx, existing := range c.ranges {
		if _, ok := remove[idx]; ok {
			continue
		}
		remaining = append(remaining, existing)
	}
	c.ranges = remaining

	first := overlapping[0]
	if bytes.Compare(first.startKey, startKey) < 0 {
		c.insertRangeLocked(&cachedRange{
			startKey:   append([]byte(nil), first.startKey...),
			limitKey:   append([]byte(nil), startKey...),
			group:      c.refGroup(first.group),
			splitID:    first.splitID,
			generation: append([]byte(nil), first.generation...),
			lastAccess: first.lastAccess,
		})
	}

	c.insertRangeLocked(&cachedRange{
		startKey:   startKey,
		limitKey:   limitKey,
		group:      c.findAndRefGroup(rangeIn.GetGroupUid()),
		splitID:    rangeIn.GetSplitId(),
		generation: append([]byte(nil), rangeIn.GetGeneration()...),
		lastAccess: c.accessTimeNowLocked(),
	})

	last := overlapping[len(overlapping)-1]
	if bytes.Compare(last.limitKey, limitKey) > 0 {
		c.insertRangeLocked(&cachedRange{
			startKey:   append([]byte(nil), limitKey...),
			limitKey:   append([]byte(nil), last.limitKey...),
			group:      c.refGroup(last.group),
			splitID:    last.splitID,
			generation: append([]byte(nil), last.generation...),
			lastAccess: last.lastAccess,
		})
	}

	for _, existing := range overlapping {
		c.unrefGroup(existing.group)
	}
}

func (c *keyRangeCache) insertRangeLocked(r *cachedRange) {
	c.ranges = append(c.ranges, r)
	sort.Slice(c.ranges, func(i, j int) bool {
		return bytes.Compare(c.ranges[i].limitKey, c.ranges[j].limitKey) < 0
	})
}

func (c *keyRangeCache) findAndRefGroup(groupUID uint64) *cachedGroup {
	group := c.groups[groupUID]
	if group != nil {
		group.refs++
	}
	return group
}

func (c *keyRangeCache) findOrInsertGroup(groupIn *sppb.Group) *cachedGroup {
	if groupIn == nil {
		return nil
	}
	group, ok := c.groups[groupIn.GetGroupUid()]
	if !ok {
		group = &cachedGroup{groupUID: groupIn.GetGroupUid(), leaderIdx: -1, refs: 1}
		c.groups[group.groupUID] = group
	} else {
		group.refs++
	}
	group.update(groupIn)
	return group
}

func (c *keyRangeCache) refGroup(group *cachedGroup) *cachedGroup {
	if group != nil {
		group.refs++
	}
	return group
}

func (c *keyRangeCache) unrefGroup(group *cachedGroup) {
	if group == nil {
		return
	}
	group.refs--
	if group.refs <= 0 {
		delete(c.groups, group.groupUID)
	}
}

func (t *cachedTablet) update(tabletIn *sppb.Tablet) {
	if tabletIn == nil {
		return
	}
	if t.tabletUID > 0 && bytes.Compare(t.incarnation, tabletIn.GetIncarnation()) > 0 {
		return
	}
	t.tabletUID = tabletIn.GetTabletUid()
	t.incarnation = append([]byte(nil), tabletIn.GetIncarnation()...)
	t.distance = tabletIn.GetDistance()
	t.skip = tabletIn.GetSkip()
	t.role = tabletIn.GetRole()
	t.location = tabletIn.GetLocation()
	if t.serverAddress != tabletIn.GetServerAddress() {
		t.serverAddress = tabletIn.GetServerAddress()
		t.endpoint = nil
	}
}

func (t *cachedTablet) matches(directedReadOptions *sppb.DirectedReadOptions) bool {
	if directedReadOptions == nil {
		return t.distance <= maxLocalReplicaDistance
	}
	switch replicas := directedReadOptions.GetReplicas().(type) {
	case *sppb.DirectedReadOptions_IncludeReplicas_:
		for _, selection := range replicas.IncludeReplicas.GetReplicaSelections() {
			if t.matchesReplicaSelection(selection) {
				return true
			}
		}
		return false
	case *sppb.DirectedReadOptions_ExcludeReplicas_:
		for _, selection := range replicas.ExcludeReplicas.GetReplicaSelections() {
			if t.matchesReplicaSelection(selection) {
				return false
			}
		}
		return true
	default:
		return t.distance <= maxLocalReplicaDistance
	}
}

func (t *cachedTablet) matchesReplicaSelection(selection *sppb.DirectedReadOptions_ReplicaSelection) bool {
	if selection == nil {
		return true
	}
	if selection.GetLocation() != "" && selection.GetLocation() != t.location {
		return false
	}
	switch selection.GetType() {
	case sppb.DirectedReadOptions_ReplicaSelection_READ_WRITE:
		return t.role == sppb.Tablet_READ_WRITE || t.role == sppb.Tablet_ROLE_UNSPECIFIED
	case sppb.DirectedReadOptions_ReplicaSelection_READ_ONLY:
		return t.role == sppb.Tablet_READ_ONLY
	default:
		return true
	}
}

func (t *cachedTablet) shouldSkip(hint *sppb.RoutingHint) bool {
	if hint == nil {
		return true
	}
	if t.skip || t.serverAddress == "" || (t.endpoint != nil && !t.endpoint.IsHealthy()) {
		hint.SkippedTabletUid = append(hint.SkippedTabletUid, &sppb.RoutingHint_SkippedTablet{
			TabletUid:   t.tabletUID,
			Incarnation: append([]byte(nil), t.incarnation...),
		})
		return true
	}
	return false
}

func (t *cachedTablet) pick(ctx context.Context, endpointCache channelEndpointCache, hint *sppb.RoutingHint) channelEndpoint {
	if hint != nil {
		hint.TabletUid = t.tabletUID
	}
	if t.endpoint == nil && t.serverAddress != "" {
		t.endpoint = endpointCache.Get(ctx, t.serverAddress)
	}
	return t.endpoint
}

func (g *cachedGroup) update(groupIn *sppb.Group) {
	if groupIn == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if bytes.Compare(groupIn.GetGeneration(), g.generation) > 0 {
		g.generation = append([]byte(nil), groupIn.GetGeneration()...)
		if idx := int(groupIn.GetLeaderIndex()); idx >= 0 && idx < len(groupIn.GetTablets()) {
			g.leaderIdx = idx
		} else {
			g.leaderIdx = -1
		}
	}

	if len(g.tablets) == len(groupIn.GetTablets()) {
		mismatch := false
		for i := range g.tablets {
			if g.tablets[i].tabletUID != groupIn.GetTablets()[i].GetTabletUid() {
				mismatch = true
				break
			}
		}
		if !mismatch {
			for i := range g.tablets {
				g.tablets[i].update(groupIn.GetTablets()[i])
			}
			return
		}
	}

	tabletByUID := make(map[uint64]*cachedTablet, len(g.tablets))
	for _, tablet := range g.tablets {
		tabletByUID[tablet.tabletUID] = tablet
	}
	newTablets := make([]*cachedTablet, 0, len(groupIn.GetTablets()))
	for _, tabletIn := range groupIn.GetTablets() {
		tablet := tabletByUID[tabletIn.GetTabletUid()]
		if tablet == nil {
			tablet = &cachedTablet{}
		}
		tablet.update(tabletIn)
		newTablets = append(newTablets, tablet)
	}
	g.tablets = newTablets
}

func (g *cachedGroup) hasLeaderLocked() bool {
	return g.leaderIdx >= 0 && g.leaderIdx < len(g.tablets)
}

func (g *cachedGroup) leaderLocked() *cachedTablet {
	if !g.hasLeaderLocked() {
		return nil
	}
	return g.tablets[g.leaderIdx]
}

func (g *cachedGroup) fillRoutingHint(ctx context.Context, endpointCache channelEndpointCache, preferLeader bool, directedReadOptions *sppb.DirectedReadOptions, hint *sppb.RoutingHint) channelEndpoint {
	g.mu.Lock()

	if directedReadOptions == nil {
		directedReadOptions = &sppb.DirectedReadOptions{}
	}
	hasDirectedReadOptions := directedReadOptions.GetReplicas() != nil

	leader := g.leaderLocked()
	if preferLeader && !hasDirectedReadOptions && leader != nil && leader.distance <= maxLocalReplicaDistance && !leader.shouldSkip(hint) {
		if leader.endpoint != nil || leader.serverAddress == "" {
			endpoint := leader.pick(ctx, endpointCache, hint)
			g.mu.Unlock()
			return endpoint
		}
		if hint != nil {
			hint.TabletUid = leader.tabletUID
		}
		serverAddress := leader.serverAddress
		g.mu.Unlock()
		endpoint := endpointCache.Get(ctx, serverAddress)
		g.mu.Lock()
		if leader.endpoint == nil && leader.serverAddress == serverAddress {
			leader.endpoint = endpoint
		}
		if hint != nil {
			hint.TabletUid = leader.tabletUID
		}
		result := leader.endpoint
		g.mu.Unlock()
		return result
	}
	for _, tablet := range g.tablets {
		if !tablet.matches(directedReadOptions) {
			continue
		}
		if tablet.shouldSkip(hint) {
			continue
		}
		if tablet.endpoint != nil || tablet.serverAddress == "" {
			endpoint := tablet.pick(ctx, endpointCache, hint)
			g.mu.Unlock()
			return endpoint
		}
		if hint != nil {
			hint.TabletUid = tablet.tabletUID
		}
		serverAddress := tablet.serverAddress
		g.mu.Unlock()
		endpoint := endpointCache.Get(ctx, serverAddress)
		g.mu.Lock()
		if tablet.endpoint == nil && tablet.serverAddress == serverAddress {
			tablet.endpoint = endpoint
		}
		if hint != nil {
			hint.TabletUid = tablet.tabletUID
		}
		result := tablet.endpoint
		g.mu.Unlock()
		return result
	}
	g.mu.Unlock()
	return nil
}
