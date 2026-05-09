// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

// +build buddy

package rudd

import (
	"fmt"
	"log"
	"sync/atomic"
	"unsafe"
)

// tables is used with the build tag buddy and corresponds to Binary Decision
// Diagrams based on the data structures and algorithms found in the BuDDy
// library.
type tables struct {
	nodes         []buddynode // List of all the BDD nodes. Constants are always kept at index 0 and 1
	freenum       int         // Number of free nodes
	freepos       int         // First free node
	produced      int         // Total number of new nodes ever produced
	nodefinalizer interface{} // Finalizer used to decrement the ref count of external references
	uniqueAccess  int         // accesses to the unique node table
	uniqueChain   int         // iterations through the cache chains in the unique node table
	uniqueHit     int         // entries actually found in the the unique node table
	uniqueMiss    int         // entries not found in the the unique node table
	gcstat                    // Information about garbage collections
	configs                   // Configurable parameters
}

type buddynode struct {
	refcou int32 // Count the number of external references
	level  int32 // Order of the variable in the BDD
	low    int   // Reference to the false branch
	high   int   // Reference to the true branch
	hash   int   // Index where to (possibly) find node with this hash value
	next   int   // Next index to check in case of a collision, 0 if last
}

func (b *tables) ismarked(n int) bool {
	return (b.nodes[n].level & 0x200000) != 0
}

func (b *tables) marknode(n int) {
	b.nodes[n].level = b.nodes[n].level | 0x200000
}

func (b *tables) unmarknode(n int) {
	b.nodes[n].level = b.nodes[n].level & 0x1FFFFF
}

// The hash function for nodes is #(level, low, high)

func (b *tables) ptrhash(n int) int {
	return _TRIPLE(int(b.nodes[n].level), b.nodes[n].low, b.nodes[n].high, len(b.nodes))
}

func (b *tables) nodehash(level int32, low, high int) int {
	return _TRIPLE(int(level), low, high, len(b.nodes))
}

// New returns a new BDD based on the implementation selected with the build
// tag; meaning the 'Hudd'-style BDD by default (based on the standard runtime
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
	nodesize := primeGte(config.nodesize)
	impl.nodes = make([]buddynode, nodesize)
	for k := range impl.nodes {
		impl.nodes[k] = buddynode{
			refcou: 0,
			level:  0,
			low:    -1,
			high:   0,
			hash:   0,
			next:   k + 1,
		}
	}
	impl.nodes[nodesize-1].next = 0
	impl.nodes[0].refcou = _MAXREFCOUNT
	impl.nodes[1].refcou = _MAXREFCOUNT
	impl.nodes[0].low = 0
	impl.nodes[0].high = 0
	impl.nodes[1].low = 1
	impl.nodes[1].high = 1
	impl.nodes[0].level = int32(config.varnum)
	impl.nodes[1].level = int32(config.varnum)
	impl.freepos = 2
	impl.freenum = nodesize - 2
	impl.gcstat.history = []gcpoint{}
	impl.nodefinalizer = func(n *int) {
		if _DEBUG {
			atomic.AddUint64(&(impl.gcstat.calledfinalizers), 1)
			if _LOGLEVEL > 2 {
				log.Printf("dec refcou %d\n", *n)
			}
		}
		impl.nodes[*n].refcou--
	}
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
	b.tables = impl
	b.cacheinit(config)
	return b, nil
}

func (b *tables) size() int {
	return len(b.nodes)
}

func (b *tables) level(n int) int32 {
	return b.nodes[n].level
}

func (b *tables) low(n int) int {
	return b.nodes[n].low
}

func (b *tables) high(n int) int {
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
	for k := range b.nodes {
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
	for k, v := range b.nodes {
		if v.low != -1 {
			if err := f(k, int(v.level), v.low, v.high); err != nil {
				return err
			}
		}
	}
	return nil
}

// Stats returns information about the BDD
func (b *tables) stats() string {
	res := "Impl.:      BuDDy\n"
	res += fmt.Sprintf("Allocated:  %d  (%s)\n", len(b.nodes), humanSize(len(b.nodes), unsafe.Sizeof(buddynode{})))
	res += fmt.Sprintf("Produced:   %d\n", b.produced)
	r := (float64(b.freenum) / float64(len(b.nodes))) * 100
	res += fmt.Sprintf("Free:       %d  (%.3g %%)\n", b.freenum, r)
	res += fmt.Sprintf("Used:       %d  (%.3g %%)\n", len(b.nodes)-b.freenum, (100.0 - r))
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
		res += fmt.Sprintf("Unique Chain:   %d\n", b.uniqueChain)
		res += fmt.Sprintf("Unique Hit:     %d (%.1f%% + %.1f%%)\n", b.uniqueHit, (float64(b.uniqueHit)*100)/float64(b.uniqueAccess),
			(float64(b.uniqueAccess-b.uniqueMiss-b.uniqueHit)*100)/float64(b.uniqueAccess))
		res += fmt.Sprintf("Unique Miss:    %d\n", b.uniqueMiss)
	}
	return res
}
