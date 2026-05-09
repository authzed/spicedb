// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

//go:build !buddy
// +build !buddy

package rudd

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"unsafe"
)

// tables corresponds to Binary Decision Diagrams based on the runtime
// hashmap. We hash a triplet (level, low, high) to a []byte and use the unique
// table to associate this triplet to an entry in the nodes table. We use more
// space but a benefit is that we can easily migrate to a concurrency-safe
// hashmap if we want to test concurrent data structures.
type tables struct {
	sync.RWMutex
	nodes         []huddnode             // List of all the BDD nodes. Constants are always kept at index 0 and 1
	unique        map[[huddsize]byte]int // Unicity table, used to associate each triplet to a single node
	freenum       int                    // Number of free nodes
	freepos       int                    // First free node
	produced      int                    // Total number of new nodes ever produced
	hbuff         [huddsize]byte         // Used to compute the hash of nodes. A Buffer needs no initialization.
	nodefinalizer interface{}            // Finalizer used to decrement the ref count of external references
	uniqueAccess  int                    // accesses to the unique node table
	uniqueHit     int                    // entries actually found in the the unique node table
	uniqueMiss    int                    // entries not found in the the unique node table
	gcstat                               // Information about garbage collections
	configs                              // Configurable parameters
}

type huddnode struct {
	level  int32 // Order of the variable in the BDD
	low    int   // Reference to the false branch
	high   int   // Reference to the true branch
	refcou int32 // Count the number of external references
}

func (b *tables) ismarked(n int) bool {
	b.RLock()
	defer b.RUnlock()
	return (b.nodes[n].refcou & 0x200000) != 0
}

func (b *tables) marknode(n int) {
	b.RLock()
	defer b.RUnlock()
	b.nodes[n].refcou |= 0x200000
}

func (b *tables) unmarknode(n int) {
	b.RLock()
	defer b.RUnlock()
	b.nodes[n].refcou &= 0x1FFFFF
}

// New returns a new BDD based on an implementation selected with the build tag;
// meaning the 'Hudd'-style BDD by default (based on the standard runtime
// hashmap) or a 'BuDDy'-style BDD if tags buddy is set. Parameter varnum is the
// number of variables in the BDD.
//
// It is possible to set optional (configuration) parameters, such as the size
// of the initial node table (Nodesize) or the size for caches (Cachesize),
// using configs functions. The initial number of nodes is not critical since
// the table will be resized whenever there are too few nodes left after a
// garbage collection. But it does have some impact on the efficiency of the
// operations. We return a nil value if there is an error while creating the
// BDD.
func New(varnum int, options ...func(*configs)) (*BDD, error) {
	b := &BDD{}
	if (varnum < 1) || (varnum > int(_MAXVAR)) {
		b.seterror("bad number of variable (%d)", varnum)
		return nil, b.error
	}
	config := makeconfigs(varnum)
	for _, f := range options {
		f(config)
	}
	b.varnum = int32(varnum)
	if _LOGLEVEL > 0 {
		log.Printf("set varnum to %d\n", b.varnum)
	}
	b.varset = make([][2]int, varnum)
	// We also initialize the refstack.
	b.refstack = make([]int, 0, 2*varnum+4)
	b.initref()
	b.error = nil
	impl := &tables{}
	impl.minfreenodes = config.minfreenodes
	impl.maxnodeincrease = config.maxnodeincrease
	// initializing the list of nodes
	nodesize := config.nodesize
	impl.nodes = make([]huddnode, nodesize)
	for k := range impl.nodes {
		impl.nodes[k] = huddnode{
			level:  0,
			low:    -1,
			high:   k + 1,
			refcou: 0,
		}
	}
	impl.nodes[nodesize-1].high = 0
	impl.unique = make(map[[huddsize]byte]int, nodesize)
	// creating bddzero and bddone. We do not add them to the unique table.
	impl.nodes[0] = huddnode{
		level:  int32(config.varnum),
		low:    0,
		high:   0,
		refcou: _MAXREFCOUNT,
	}
	impl.nodes[1] = huddnode{
		level:  int32(config.varnum),
		low:    1,
		high:   1,
		refcou: _MAXREFCOUNT,
	}
	impl.freepos = 2
	impl.freenum = len(impl.nodes) - 2
	for k := 0; k < config.varnum; k++ {
		v0, _ := impl.makenode(int32(k), 0, 1, nil)
		if v0 < 0 {
			b.seterror("cannot allocate new variable %d in setVarnum", k)
			return nil, b.error
		}
		impl.nodes[v0].refcou = _MAXREFCOUNT
		b.pushref(v0)
		v1, _ := impl.makenode(int32(k), 1, 0, nil)
		if v1 < 0 {
			b.seterror("cannot allocate new variable %d in setVarnum", k)
			return nil, b.error
		}
		impl.nodes[v1].refcou = _MAXREFCOUNT
		b.popref(1)
		b.varset[k] = [2]int{v0, v1}
	}
	impl.gcstat.history = []gcpoint{}
	impl.nodefinalizer = func(n *int) {
		b.Lock()
		defer b.Unlock()
		if _DEBUG {
			atomic.AddUint64(&(impl.gcstat.calledfinalizers), 1)
			if _LOGLEVEL > 2 {
				log.Printf("dec refcou %d\n", *n)
			}
		}
		impl.nodes[*n].refcou--
	}
	b.tables = impl
	b.cacheinit(config)
	return b, nil
}

func (b *tables) huddhash(level int32, low, high int) {
	b.hbuff[0] = byte(level)
	b.hbuff[1] = byte(level >> 8)
	b.hbuff[2] = byte(level >> 16)
	b.hbuff[3] = byte(level >> 24)
	b.hbuff[4] = byte(low)
	b.hbuff[5] = byte(low >> 8)
	b.hbuff[6] = byte(low >> 16)
	b.hbuff[7] = byte(low >> 24)
	if huddsize == 20 {
		// 64 bits machine
		b.hbuff[8] = byte(low >> 32)
		b.hbuff[9] = byte(low >> 40)
		b.hbuff[10] = byte(low >> 48)
		b.hbuff[11] = byte(low >> 56)
		b.hbuff[12] = byte(high)
		b.hbuff[13] = byte(high >> 8)
		b.hbuff[14] = byte(high >> 16)
		b.hbuff[15] = byte(high >> 24)
		b.hbuff[16] = byte(high >> 32)
		b.hbuff[17] = byte(high >> 40)
		b.hbuff[18] = byte(high >> 48)
		b.hbuff[19] = byte(high >> 56)
		return
	}
	// 32 bits machine
	b.hbuff[8] = byte(high)
	b.hbuff[9] = byte(high >> 8)
	b.hbuff[10] = byte(high >> 16)
	b.hbuff[11] = byte(high >> 24)
}

func (b *tables) nodehash(level int32, low, high int) (int, bool) {
	b.huddhash(level, low, high)
	hn, ok := b.unique[b.hbuff]
	return hn, ok
}

// When a slot is unused in b.nodes, we have low set to -1 and high set to the
// next free position. The value of b.freepos gives the index of the lowest
// unused slot, except when freenum is 0, in which case it is also 0.

func (b *tables) setnode(level int32, low int, high int, count int32) int {
	b.Lock()
	defer b.Unlock()
	b.huddhash(level, low, high)
	b.freenum--
	b.unique[b.hbuff] = b.freepos
	res := b.freepos
	b.freepos = b.nodes[b.freepos].high
	b.nodes[res] = huddnode{level, low, high, count}
	return res
}

func (b *tables) delnode(hn huddnode) {
	b.huddhash(hn.level, hn.low, hn.high)
	delete(b.unique, b.hbuff)
}

func (b *tables) size() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.nodes)
}

func (b *tables) level(n int) int32 {
	b.RLock()
	defer b.RUnlock()
	return b.nodes[n].level
}

func (b *tables) low(n int) int {
	b.RLock()
	defer b.RUnlock()
	return b.nodes[n].low
}

func (b *tables) high(n int) int {
	b.RLock()
	defer b.RUnlock()
	return b.nodes[n].high
}

func (b *tables) allnodesfrom(f func(id, level, low, high int) error, n []Node) error {
	for _, v := range n {
		b.markrec(*v)
	}
	// if err := f(0, int(b.nodes[0].level), 0, 0); err != nil {
	// 	b.unmarkall()
	// 	return err
	// }
	// if err := f(1, int(b.nodes[1].level), 1, 1); err != nil {
	// 	b.unmarkall()
	// 	return err
	// }
	b.RLock()
	count := len(b.nodes)
	b.RUnlock()

	for k := 0; k < count; k++ {
		b.RLock()
		if k >= len(b.nodes) {
			break
		}
		b.RUnlock()
		if b.ismarked(k) {
			b.unmarknode(k)
			if err := f(k, int(b.nodes[k].level), b.nodes[k].low, b.nodes[k].high); err != nil {
				b.unmarkall()
				return err
			}
		}
	}
	return nil
}

func (b *tables) allnodes(f func(id, level, low, high int) error) error {
	// if err := f(0, int(b.nodes[0].level), 0, 0); err != nil {
	// 	return err
	// }
	// if err := f(1, int(b.nodes[1].level), 1, 1); err != nil {
	// 	return err
	// }
	b.RLock()
	count := len(b.nodes)
	b.RUnlock()

	for k := 0; k < count; k++ {
		b.RLock()
		if k >= len(b.nodes) {
			break
		}
		v := b.nodes[k]
		b.RUnlock()
		if v.low != -1 {
			if err := f(k, int(v.level), v.low, v.high); err != nil {
				return err
			}
		}
	}
	return nil
}

// stats returns information about the implementation
func (b *tables) stats() string {
	b.RLock()
	defer b.RUnlock()
	res := "Impl.:      Hudd\n"
	res += fmt.Sprintf("Allocated:  %d (%s)\n", len(b.nodes), humanSize(len(b.nodes), unsafe.Sizeof(huddnode{})))
	res += fmt.Sprintf("Produced:   %d\n", b.produced)
	r := (float64(b.freenum) / float64(len(b.nodes))) * 100
	res += fmt.Sprintf("Free:       %d (%.3g %%)\n", b.freenum, r)
	res += fmt.Sprintf("Used:       %d (%.3g %%)\n", len(b.nodes)-b.freenum, (100.0 - r))
	res += "==============\n"
	res += fmt.Sprintf("# of GC:    %d\n", len(b.gcstat.history))
	if _DEBUG {
		allocated := int(b.gcstat.setfinalizers)
		reclaimed := int(b.gcstat.calledfinalizers)
		for _, g := range b.gcstat.history {
			allocated += g.setfinalizers
			reclaimed += g.calledfinalizers
		}
		res += fmt.Sprintf("Ext. refs:  %d\n", allocated)
		res += fmt.Sprintf("Reclaimed:  %d\n", reclaimed)
		res += "==============\n"
		res += fmt.Sprintf("Unique Access:  %d\n", b.uniqueAccess)
		res += fmt.Sprintf("Unique Hit:     %d (%.1f%% + %.1f%%)\n", b.uniqueHit, (float64(b.uniqueHit)*100)/float64(b.uniqueAccess),
			(float64(b.uniqueAccess-b.uniqueMiss-b.uniqueHit)*100)/float64(b.uniqueAccess))
		res += fmt.Sprintf("Unique Miss:    %d\n", b.uniqueMiss)
	}
	return res
}
