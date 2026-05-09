// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

import (
	"fmt"
	"math"
	"unsafe"
)

// Hash functions

func _TRIPLE(a, b, c, len int) int {
	return int(_PAIR(c, _PAIR(a, b, len), len))
}

// _PAIR is a mapping function that maps (bijectively) a pair of integer (a, b)
// into a unique integer then cast it into a value in the interval [0..len)
// using a modulo operation.
func _PAIR(a, b, len int) int {
	ua := uint64(a)
	ub := uint64(b)
	return int(((((ua + ub) * (ua + ub + 1)) / 2) + (ua)) % uint64(len))
}

// Hash value modifiers for replace/compose
const cacheidREPLACE int = 0x0

// const cacheid_COMPOSE int = 0x1
// const cacheid_VECCOMPOSE int = 0x2

// Hash value modifiers for quantification
const cacheidEXIST int = 0x0
const cacheidAPPEX int = 0x3

// const cacheid_FORALL int = 0x1
// const cacheid_UNIQUE int = 0x2
// const cacheid_APPAL int = 0x4
// const cacheid_APPUN int = 0x5

type data4n struct {
	res int
	a   int
	b   int
	c   int
}

type data4ncache struct {
	ratio  int
	opHit  int // entries found in the caches
	opMiss int // entries not found in the caches
	table  []data4n
}

func (bc *data4ncache) init(size, ratio int) {
	size = primeGte(size)
	bc.table = make([]data4n, size)
	bc.ratio = ratio
	bc.reset()
}

func (bc *data4ncache) resize(size int) {
	if bc.ratio > 0 {
		size = primeGte((size * bc.ratio) / 100)
		bc.table = make([]data4n, size)
	}
	bc.reset()
}

func (bc *data4ncache) reset() {
	for k := range bc.table {
		bc.table[k].a = -1
	}
}

// cache3n is used for caching replace operations
type data3ncache struct {
	ratio  int
	opHit  int // entries found in the replace cache
	opMiss int // entries not found in the replace cache
	table  []data3n
}

type data3n struct {
	res int
	a   int
	c   int
}

func (bc *data3ncache) init(size, ratio int) {
	size = primeGte(size)
	bc.table = make([]data3n, size)
	bc.ratio = ratio
	bc.reset()
}

func (bc *data3ncache) resize(size int) {
	if bc.ratio > 0 {
		size = primeGte((size * bc.ratio) / 100)
		bc.table = make([]data3n, size)
	}
	bc.reset()
}

func (bc *data3ncache) reset() {
	for k := range bc.table {
		bc.table[k].a = -1
	}
}

// Setup and shutdown

func (b *BDD) cacheinit(c *configs) {
	size := 10000
	if c.cachesize != 0 {
		size = c.cachesize
	}
	size = primeGte(size)
	b.applycache = &applycache{}
	b.applycache.init(size, c.cacheratio)
	b.itecache = &itecache{}
	b.itecache.init(size, c.cacheratio)
	b.quantcache = &quantcache{}
	b.quantcache.init(size, c.cacheratio)
	b.quantset = make([]int32, b.varnum)
	b.quantsetID = 0
	b.appexcache = &appexcache{}
	b.appexcache.init(size, c.cacheratio)
	b.replacecache = &replacecache{}
	b.replacecache.init(size, c.cacheratio)
}

func (b *BDD) cachereset() {
	b.applycache.reset()
	b.itecache.reset()
	b.quantcache.reset()
	b.appexcache.reset()
	b.replacecache.reset()
}

func (b *BDD) cacheresize(nodesize int) {
	b.applycache.resize(nodesize)
	b.itecache.resize(nodesize)
	b.quantcache.resize(nodesize)
	b.appexcache.resize(nodesize)
	b.replacecache.resize(nodesize)
}

//
// Quantification Cache
//

// quantset2cache takes a variable list, similar to the ones generated with
// Makeset, and set the variables in the quantification cache.
func (b *BDD) quantset2cache(n int) error {
	if n < 2 {
		b.seterror("Illegal variable (%d) in varset to cache", n)
		return b.error
	}
	b.quantsetID++
	if b.quantsetID == math.MaxInt32 {
		b.quantset = make([]int32, b.varnum)
		b.quantsetID = 1
	}
	for i := n; i > 1; i = b.high(i) {
		b.quantset[b.level(i)] = b.quantsetID
		b.quantlast = b.level(i)
	}
	return nil
}

// The hash function for Apply is #(left, right, applycache.op).

type applycache struct {
	data4ncache
	op int // Current operation during an apply
}

func (bc *applycache) matchapply(left, right int) int {
	entry := bc.table[_TRIPLE(left, right, bc.op, len(bc.table))]
	if entry.a == left && entry.b == right && entry.c == bc.op {
		if _DEBUG {
			bc.opHit++
		}
		return entry.res
	}
	if _DEBUG {
		bc.opMiss++
	}
	return -1
}

func (bc *applycache) setapply(left, right, res int) int {
	bc.table[_TRIPLE(left, right, bc.op, len(bc.table))] = data4n{
		a:   left,
		b:   right,
		c:   bc.op,
		res: res,
	}
	return res
}

// The hash function for operation Not(n) is simply n.

func (bc *applycache) matchnot(n int) int {
	entry := bc.table[n%len(bc.table)]
	if entry.a == n && entry.c == int(opnot) {
		if _DEBUG {
			bc.opHit++
		}
		return entry.res
	}
	if _DEBUG {
		bc.opMiss++
	}
	return -1
}

func (bc *applycache) setnot(n, res int) int {
	bc.table[n%len(bc.table)] = data4n{
		a:   n,
		c:   int(opnot),
		res: res,
	}
	return res
}

func (bc applycache) String() string {
	res := fmt.Sprintf("== Apply cache  %d (%s)\n", len(bc.table), humanSize(len(bc.table), unsafe.Sizeof(data4n{})))
	res += fmt.Sprintf(" Operator Hits: %d (%.1f%%)\n", bc.opHit, (float64(bc.opHit)*100)/(float64(bc.opHit)+float64(bc.opMiss)))
	res += fmt.Sprintf(" Operator Miss: %d\n", bc.opMiss)
	return res
}

// The hash function for ITE is #(f,g,h), so we need to cache 4 node positions
// per entry.

type itecache struct {
	data4ncache
}

func (bc *itecache) matchite(f, g, h int) int {
	entry := bc.table[_TRIPLE(f, g, h, len(bc.table))]
	if entry.a == f && entry.b == g && entry.c == h {
		if _DEBUG {
			bc.opHit++
		}
		return entry.res
	}
	if _DEBUG {
		bc.opMiss++
	}
	return -1
}

func (bc *itecache) setite(f, g, h, res int) int {
	bc.table[_TRIPLE(f, g, h, len(bc.table))] = data4n{
		a:   f,
		b:   g,
		c:   h,
		res: res,
	}
	return res
}

func (bc itecache) String() string {
	res := fmt.Sprintf("== ITE cache    %d (%s)\n", len(bc.table), humanSize(len(bc.table), unsafe.Sizeof(data4n{})))
	res += fmt.Sprintf(" Operator Hits: %d (%.1f%%)\n", bc.opHit, (float64(bc.opHit)*100)/(float64(bc.opHit)+float64(bc.opMiss)))
	res += fmt.Sprintf(" Operator Miss: %d\n", bc.opMiss)
	return res
}

// The hash function for quantification is (n, varset, quantid).

type quantcache struct {
	data4ncache         // Cache for exist/forall results
	quantset    []int32 // Current variable set for quant.
	quantsetID  int32   // Current id used in quantset
	quantlast   int32   // Current last variable to be quant.
	id          int     // Current cache id for quantifications
}

func (bc *quantcache) matchquant(n, varset int) int {
	entry := bc.table[_PAIR(n, varset, len(bc.table))]
	if entry.a == n && entry.b == varset && entry.c == bc.id {
		if _DEBUG {
			bc.opHit++
		}
		return entry.res
	}
	if _DEBUG {
		bc.opMiss++
	}
	return -1
}

func (bc *quantcache) setquant(n, varset, res int) int {
	bc.table[_PAIR(n, varset, len(bc.table))] = data4n{
		a:   n,
		b:   varset,
		c:   bc.id,
		res: res,
	}
	return res
}

func (bc quantcache) String() string {
	res := fmt.Sprintf("== Quant cache  %d (%s)\n", len(bc.table), humanSize(len(bc.table), unsafe.Sizeof(data4n{})))
	res += fmt.Sprintf(" Operator Hits: %d (%.1f%%)\n", bc.opHit, (float64(bc.opHit)*100)/(float64(bc.opHit)+float64(bc.opMiss)))
	res += fmt.Sprintf(" Operator Miss: %d\n", bc.opMiss)
	return res
}

// The hash function for AppEx is #(left, right, varset << 2 | appexcache.op )
// so we can use the same cache for several operators.

// appexcache are a mix of  quant and apply caches
type appexcache struct {
	data4ncache     // Cache for appex/appall results
	op          int // Current operator for appex
	id          int // Current id
}

func (bc *appexcache) matchappex(left, right int) int {
	entry := bc.table[_TRIPLE(left, right, bc.id, len(bc.table))]
	if entry.a == left && entry.b == right && entry.c == bc.id {
		if _DEBUG {
			bc.opHit++
		}
		return entry.res
	}
	if _DEBUG {
		bc.opMiss++
	}
	return -1
}

func (bc *appexcache) setappex(left, right, res int) int {
	bc.table[_TRIPLE(left, right, bc.id, len(bc.table))] = data4n{
		a:   left,
		b:   right,
		c:   bc.id,
		res: res,
	}
	return res
}

func (bc appexcache) String() string {
	res := fmt.Sprintf("== AppEx cache  %d (%s)\n", len(bc.table), humanSize(len(bc.table), unsafe.Sizeof(data4n{})))
	res += fmt.Sprintf(" Operator Hits: %d (%.1f%%)\n", bc.opHit, (float64(bc.opHit)*100)/(float64(bc.opHit)+float64(bc.opMiss)))
	res += fmt.Sprintf(" Operator Miss: %d\n", bc.opMiss)
	return res
}

// The hash function for operation Replace(n) is simply n.

type replacecache struct {
	data3ncache     // Cache for replace results
	id          int // Current cache id for replace
}

func (bc *replacecache) matchreplace(n int) int {
	entry := bc.table[n%len(bc.table)]
	if entry.a == n && entry.c == bc.id {
		if _DEBUG {
			bc.opHit++
		}
		return entry.res
	}
	if _DEBUG {
		bc.opMiss++
	}
	return -1
}

func (bc *replacecache) setreplace(n, res int) int {
	bc.table[n%len(bc.table)] = data3n{
		a:   n,
		c:   bc.id,
		res: res,
	}
	return res
}

func (bc replacecache) String() string {
	res := fmt.Sprintf("== Replace      %d (%s)\n", len(bc.table), humanSize(len(bc.table), unsafe.Sizeof(data3n{})))
	res += fmt.Sprintf(" Operator Hits: %d (%.1f%%)\n", bc.opHit, (float64(bc.opHit)*100)/(float64(bc.opHit)+float64(bc.opMiss)))
	res += fmt.Sprintf(" Operator Miss: %d\n", bc.opMiss)
	return res
}
