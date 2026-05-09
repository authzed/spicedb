// Package hashring implements a thread-safe consistent hashring with a
// pluggable hashing algorithm.
//
// This package was developed for use in a gRPC balancer, but nothing precludes
// it from being used for any other purpose.
package hashring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"golang.org/x/exp/slices"
)

var (
	ErrMemberAlreadyExists      = errors.New("member node already exists")
	ErrMemberNotFound           = errors.New("member node not found")
	ErrNotEnoughMembers         = errors.New("not enough member nodes to satisfy request")
	ErrInvalidReplicationFactor = errors.New("replication factor must be at least 1")
	ErrVnodeNotFound            = errors.New("vnode not found")
	ErrUnexpectedVnodeCount     = errors.New("found a different number of vnodes than replication factor")
)

// HashFunc is the signature for any hashing function that can be leveraged by
// the hashring.
type HashFunc func([]byte) uint64

// Member represents a participating member of the hashring.
// In most use cases, you can think of a member as a node or backend.
type Member interface {
	Key() string
}

// Ring provides a thread-safe consistent hashring implementation with a
// configurable number of virtual nodes.
type Ring struct {
	hashfn            HashFunc
	replicationFactor uint16

	sync.RWMutex
	nodes        map[string]nodeRecord
	virtualNodes []virtualNode
}

// MustNew creates a new Hashring with the specified hasher function and
// replication factor.
//
// If the provided replication factor is less than 1, this function will panic.
func MustNew(hasher HashFunc, replicationFactor uint16) *Ring {
	hr, err := New(hasher, replicationFactor)
	if err != nil {
		panic(err)
	}

	return hr
}

// New allocates a Ring with the specified hash function and replication factor.
//
// The replication factor must be greater than 0 and ideally be at least 20 or
// higher for quality key distribution. At 100, the standard distribution of
// key->member mapping will be about 10% of the mean. At 1000, it will be about
// 3.2%. This value should be chosen very carefully because a higher value will
// require more memory and decrease member selection performance.
func New(hashfn HashFunc, replicationFactor uint16) (*Ring, error) {
	if replicationFactor < 1 {
		return nil, ErrInvalidReplicationFactor
	}

	return &Ring{
		hashfn:            hashfn,
		replicationFactor: replicationFactor,
		nodes:             map[string]nodeRecord{},
	}, nil
}

// Add inserts a member into the hashring.
//
// If a member with the same key is already in the hashring,
// ErrMemberAlreadyExists is returned.
func (h *Ring) Add(member Member) error {
	nodeKeyString := member.Key()
	nodeHash := h.hashfn([]byte(nodeKeyString))
	newNodeRecord := nodeRecord{
		nodeHash,
		nodeKeyString,
		member,
		nil,
	}

	h.Lock()
	defer h.Unlock()

	if _, ok := h.nodes[nodeKeyString]; ok {
		return ErrMemberAlreadyExists
	}

	// virtualNodeBuffer is a 10-byte array, where 8 bytes are the hash value of
	// the member key, and the final 2 bytes are an offset of the virtual node
	// itself. This value is then hashed to get the final hash value of the virtual node.
	virtualNodeBuffer := make([]byte, 10)
	binary.LittleEndian.PutUint64(virtualNodeBuffer, nodeHash)

	for i := uint16(0); i < h.replicationFactor; i++ {
		binary.LittleEndian.PutUint16(virtualNodeBuffer[8:], i)
		virtualNodeHash := h.hashfn(virtualNodeBuffer)

		virtualNode := virtualNode{
			virtualNodeHash,
			newNodeRecord,
		}

		newNodeRecord.virtualNodes = append(newNodeRecord.virtualNodes, virtualNode)
		h.virtualNodes = append(h.virtualNodes, virtualNode)
	}

	slices.SortFunc(h.virtualNodes, cmpVnode)

	// Add the node to our map of nodes
	h.nodes[nodeKeyString] = newNodeRecord

	return nil
}

// Remove finds and removes the specified member from the hashring.
//
// If no member can be found, ErrMemberNotFound is returned.
func (h *Ring) Remove(member Member) error {
	nodeKeyString := member.Key()

	h.Lock()
	defer h.Unlock()

	foundNode, ok := h.nodes[nodeKeyString]
	if !ok {
		return ErrMemberNotFound
	}

	indexesToRemove := make([]int, 0, h.replicationFactor)
	for _, vnode := range foundNode.virtualNodes {
		vnode := vnode
		vnodeIndex := sort.Search(len(h.virtualNodes), func(i int) bool {
			return cmpVnode(h.virtualNodes[i], vnode) >= 0
		})
		if vnodeIndex >= len(h.virtualNodes) {
			return fmt.Errorf(
				"failed to delete vnode %020d/%020d/%s: %w",
				vnode.hashvalue,
				vnode.members.hashvalue,
				vnode.members.nodeKey,
				ErrVnodeNotFound,
			)
		}

		indexesToRemove = append(indexesToRemove, vnodeIndex)
	}

	sort.Slice(indexesToRemove, func(i, j int) bool {
		// NOTE: this is a reverse sort!
		return indexesToRemove[j] < indexesToRemove[i]
	})

	if len(indexesToRemove) != int(h.replicationFactor) {
		return ErrUnexpectedVnodeCount
	}

	for i, indexToRemove := range indexesToRemove {
		// Swap this index for a later one
		h.virtualNodes[indexToRemove] = h.virtualNodes[len(h.virtualNodes)-1-i]
	}

	// Truncate and sort the nodelist
	h.virtualNodes = h.virtualNodes[:len(h.virtualNodes)-len(indexesToRemove)]
	slices.SortFunc(h.virtualNodes, cmpVnode)

	// Remove the node from our map
	delete(h.nodes, nodeKeyString)

	return nil
}

// FindN finds the first N members after the specified key.
//
// If there are not enough members to satisfy the request, ErrNotEnoughMembers
// is returned.
func (h *Ring) FindN(key []byte, num uint8) ([]Member, error) {
	h.RLock()
	defer h.RUnlock()

	if int(num) > len(h.nodes) {
		return nil, ErrNotEnoughMembers
	}

	keyHash := h.hashfn(key)

	vnodeIndex := sort.Search(len(h.virtualNodes), func(i int) bool {
		return h.virtualNodes[i].hashvalue >= keyHash
	})

	alreadyFoundNodeKeys := map[string]struct{}{}
	foundNodes := make([]Member, 0, num)
	for i := 0; i < len(h.virtualNodes) && len(foundNodes) < int(num); i++ {
		boundedIndex := (i + vnodeIndex) % len(h.virtualNodes)
		candidate := h.virtualNodes[boundedIndex]
		if _, ok := alreadyFoundNodeKeys[candidate.members.nodeKey]; !ok {
			foundNodes = append(foundNodes, candidate.members.member)
			alreadyFoundNodeKeys[candidate.members.nodeKey] = struct{}{}
		}
	}

	return foundNodes, nil
}

// Members enumerates the full set of hashring members.
func (h *Ring) Members() []Member {
	h.RLock()
	defer h.RUnlock()

	membersCopy := make([]Member, 0, len(h.nodes))
	for _, nodeInfo := range h.nodes {
		membersCopy = append(membersCopy, nodeInfo.member)
	}
	return membersCopy
}

type nodeRecord struct {
	hashvalue    uint64
	nodeKey      string
	member       Member
	virtualNodes []virtualNode
}

type virtualNode struct {
	hashvalue uint64
	members   nodeRecord
}

// compareUint64 should be replaced with the standard library's cmp.Compare once
// Go 1.21 is released.
func compareUint64(x, y uint64) int {
	if x < y {
		return -1
	}
	if x > y {
		return +1
	}
	return 0
}

func cmpVnode(a, b virtualNode) int {
	if a.hashvalue == b.hashvalue {
		if a.members.hashvalue == b.members.hashvalue {
			return strings.Compare(a.members.nodeKey, b.members.nodeKey)
		}
		return compareUint64(a.members.hashvalue, b.members.hashvalue)
	}
	return compareUint64(a.hashvalue, b.hashvalue)
}
