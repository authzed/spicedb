// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

import (
	"fmt"
	"log"
	"math/big"
)

// Scanset returns the set of variables (levels) found when following the high
// branch of node n. This is the dual of function Makeset. The result may be nil
// if there is an error and it is sorted following the natural order between
// levels.
func (b *BDD) Scanset(n Node) []int {
	if b.checkptr(n) != nil {
		return nil
	}
	if *n < 2 {
		return nil
	}
	res := []int{}
	for i := *n; i > 1; i = b.high(i) {
		res = append(res, int(b.level(i)))
	}
	return res
}

// Makeset returns a node corresponding to the conjunction (the cube) of all the
// variable in varset, in their positive form. It is such that
// scanset(Makeset(a)) == a. It returns False and sets the error condition in b
// if one of the variables is outside the scope of the BDD (see documentation
// for function *Ithvar*).
func (b *BDD) Makeset(varset []int) Node {
	res := bddone
	for _, level := range varset {
		// FIXME: should find a way to do it without adding references
		tmp := b.Apply(res, b.Ithvar(level), OPand)
		if b.error != nil {
			return bddzero
		}
		res = tmp
	}
	return res
}

// Not returns the negation of the expression corresponding to node n; it
// computes the result of !n. We negate a BDD by exchanging all references to
// the zero-terminal with references to the one-terminal and vice versa.
func (b *BDD) Not(n Node) Node {
	if b.checkptr(n) != nil {
		return b.seterror("Wrong operand in call to Not (%d)", *n)
	}
	b.initref()
	b.pushref(*n)
	res := b.not(*n)
	b.popref(1)
	return b.retnode(res)
}

func (b *BDD) not(n int) int {
	if n == 0 {
		return 1
	}
	if n == 1 {
		return 0
	}
	// The hash for a not operation is simply n
	if res := b.matchnot(n); res >= 0 {
		return res
	}
	low := b.pushref(b.not(b.low(n)))
	high := b.pushref(b.not(b.high(n)))
	res := b.makenode(b.level(n), low, high)
	b.popref(2)
	return b.setnot(n, res)
}

// Apply performs all of the basic bdd operations with two operands, such as
// AND, OR etc. Operator opr must be one of the following:
//
//    Identifier    Description             Truth table
//
//    OPand         logical and              [0,0,0,1]
//    OPxor         logical xor              [0,1,1,0]
//    OPor          logical or               [0,1,1,1]
//    OPnand        logical not-and          [1,1,1,0]
//    OPnor         logical not-or           [1,0,0,0]
//    OPimp         implication              [1,1,0,1]
//    OPbiimp       equivalence              [1,0,0,1]
//    OPdiff        set difference           [0,0,1,0]
//    OPless        less than                [0,1,0,0]
//    OPinvimp      reverse implication      [1,0,1,1]
func (b *BDD) Apply(n1, n2 Node, op Operator) Node {
	if b.checkptr(n1) != nil {
		return b.seterror("Wrong operand in call to Apply %s(n1: %d, n2: ...)", op, *n1)
	}
	if b.checkptr(n2) != nil {
		return b.seterror("Wrong operand in call to Apply %s(n1: ..., n2: %d)", op, *n2)
	}
	b.applycache.op = int(op)
	b.initref()
	b.pushref(*n1)
	b.pushref(*n2)
	res := b.apply(*n1, *n2)
	b.popref(2)
	return b.retnode(res)
}

func (b *BDD) apply(left int, right int) int {
	switch Operator(b.applycache.op) {
	case OPand:
		if left == right {
			return left
		}
		if (left == 0) || (right == 0) {
			return 0
		}
		if left == 1 {
			return right
		}
		if right == 1 {
			return left
		}
	case OPor:
		if left == right {
			return left
		}
		if (left == 1) || (right == 1) {
			return 1
		}
		if left == 0 {
			return right
		}
		if right == 0 {
			return left
		}
	case OPxor:
		if left == right {
			return 0
		}
		if left == 0 {
			return right
		}
		if right == 0 {
			return left
		}
	case OPnand:
		if (left == 0) || (right == 0) {
			return 1
		}
	case OPnor:
		if (left == 1) || (right == 1) {
			return 0
		}
	case OPimp:
		if left == 0 {
			return 1
		}
		if left == 1 {
			return right
		}
		if right == 1 {
			return 1
		}
		if left == right {
			return 1
		}
	case OPbiimp:
		if left == right {
			return 1
		}
		if left == 1 {
			return right
		}
		if right == 1 {
			return left
		}
	case OPdiff:
		if left == right {
			return 0
		}
		if right == 1 {
			return 0
		}
		if left == 0 {
			return right
		}
	case OPless:
		if (left == right) || (left == 1) {
			return 0
		}
		if left == 0 {
			return right
		}
	case OPinvimp:
		if right == 0 {
			return 1
		}
		if right == 1 {
			return left
		}
		if left == 1 {
			return 1
		}
		if left == right {
			return 1
		}
	default:
		// unary operations, OPnot and OPsimplify, should not be used in apply
		b.seterror("Unauthorized operation (%s) in apply", Operator(b.applycache.op))
		return -1
	}

	// we check for errors
	if left < 0 || right < 0 {
		if _DEBUG {
			log.Panicf("panic in apply(%d,%d,%s)\n", left, right, Operator(b.applycache.op))
		}
		return -1
	}

	// we deal with the other cases where the two operands are constants
	if (left < 2) && (right < 2) {
		return opres[b.applycache.op][left][right]
	}
	if res := b.matchapply(left, right); res >= 0 {
		return res
	}
	leftlvl := b.level(left)
	rightlvl := b.level(right)
	var res int
	if leftlvl == rightlvl {
		low := b.pushref(b.apply(b.low(left), b.low(right)))
		high := b.pushref(b.apply(b.high(left), b.high(right)))
		res = b.makenode(leftlvl, low, high)
	} else {
		if leftlvl < rightlvl {
			low := b.pushref(b.apply(b.low(left), right))
			high := b.pushref(b.apply(b.high(left), right))
			res = b.makenode(leftlvl, low, high)
		} else {
			low := b.pushref(b.apply(left, b.low(right)))
			high := b.pushref(b.apply(left, b.high(right)))
			res = b.makenode(rightlvl, low, high)
		}
	}
	b.popref(2)
	return b.setapply(left, right, res)
}

// Ite (short for if-then-else operator) computes the BDD for the expression [(f
// & g) | (!f & h)] more efficiently than doing the three operations separately.
func (b *BDD) Ite(f, g, h Node) Node {
	if b.checkptr(f) != nil {
		return b.seterror("Wrong operand in call to Ite (f: %d)", *f)
	}
	if b.checkptr(g) != nil {
		return b.seterror("Wrong operand in call to Ite (g: %d)", *g)
	}
	if b.checkptr(h) != nil {
		return b.seterror("Wrong operand in call to Ite (h: %d)", *h)
	}
	b.initref()
	b.pushref(*f)
	b.pushref(*g)
	b.pushref(*h)
	res := b.ite(*f, *g, *h)
	b.popref(3)
	return b.retnode(res)
}

// iteLow returns p if p is strictly higher than q or r, otherwise it returns
// p.low. This is used in function ite to know which node to follow: we always
// follow the smallest(s) nodes.
func (b *BDD) iteLow(p, q, r int32, n int) int {
	if (p > q) || (p > r) {
		return n
	}
	return b.low(n)
}

func (b *BDD) iteHigh(p, q, r int32, n int) int {
	if (p > q) || (p > r) {
		return n
	}
	return b.high(n)
}

// min3 returns the smallest value between p, q and r. This is used in function
// ite to compute the smallest level.
func min3(p, q, r int32) int32 {
	if p <= q {
		if p <= r { // p <= q && p <= r
			return p
		}
		return r // r < p <= q
	}
	if q <= r { // q < p && q <= r
		return q
	}
	return r // r < q < p
}

func (b *BDD) ite(f, g, h int) int {
	switch {
	case f == 1:
		return g
	case f == 0:
		return h
	case g == h:
		return g
	case (g == 1) && (h == 0):
		return f
	case (g == 0) && (h == 1):
		return b.not(f)
	}
	// we check for possible errors
	if f < 0 || g < 0 || h < 0 {
		b.seterror("unexpected error in ite")
		if _DEBUG {
			log.Panicf("panic in ite(%d,%d,%d)\n", f, g, h)
		}
		return -1
	}
	if res := b.matchite(f, g, h); res >= 0 {
		return res
	}
	p := b.level(f)
	q := b.level(g)
	r := b.level(h)
	low := b.pushref(b.ite(b.iteLow(p, q, r, f), b.iteLow(q, p, r, g), b.iteLow(r, p, q, h)))
	high := b.pushref(b.ite(b.iteHigh(p, q, r, f), b.iteHigh(q, p, r, g), b.iteHigh(r, p, q, h)))
	res := b.makenode(min3(p, q, r), low, high)
	b.popref(2)
	return b.setite(f, g, h, res)
}

// Exist returns the existential quantification of n for the variables in
// varset, where varset is a node built with a method such as Makeset. We return
// nil and set the error flag in b if there is an error.
func (b *BDD) Exist(n, varset Node) Node {
	if b.checkptr(n) != nil {
		return b.seterror("Wrong node in call to Exist (n: %d)", *n)
	}
	if b.checkptr(varset) != nil {
		return b.seterror("Wrong varset in call to Exist (%d)", *varset)
	}
	if err := b.quantset2cache(*varset); err != nil {
		return nil
	}
	if *varset < 2 { // we have an empty set or a constant
		return n
	}

	b.quantcache.id = cacheidEXIST
	b.applycache.op = int(OPor)
	b.initref()
	b.pushref(*n)
	b.pushref(*varset)
	res := b.quant(*n, *varset)
	b.popref(2)
	return b.retnode(res)
}

func (b *BDD) quant(n, varset int) int {
	if (n < 2) || (b.level(n) > b.quantlast) {
		return n
	}
	// the hash for a quantification operation is simply n
	if res := b.matchquant(n, varset); res >= 0 {
		return res
	}
	low := b.pushref(b.quant(b.low(n), varset))
	high := b.pushref(b.quant(b.high(n), varset))
	var res int
	if b.quantset[b.level(n)] == b.quantsetID {
		res = b.apply(low, high)
	} else {
		res = b.makenode(b.level(n), low, high)
	}
	b.popref(2)
	return b.setquant(n, varset, res)
}

// AppEx applies the binary operator *op* on the two operands, n1 and n2, then
// performs an existential quantification over the variables in varset; meaning
// it computes the value of (∃ varset . n1 op n2). This is done in a bottom up
// manner such that both the apply and quantification is done on the lower nodes
// before stepping up to the higher nodes. This makes AppEx much more efficient
// than an apply operation followed by a quantification. Note that, when *op* is
// a conjunction, this operation returns the relational product of two BDDs.
func (b *BDD) AppEx(n1, n2 Node, op Operator, varset Node) Node {
	// FIXME: should check that op is a binary operation
	if int(op) > 3 {
		return b.seterror("operator %s not supported in call to AppEx")
	}
	if b.checkptr(varset) != nil {
		return b.seterror("wrong varset in call to AppEx (%d)", *varset)
	}
	if *varset < 2 { // we have an empty set
		return b.Apply(n1, n2, op)
	}
	if b.checkptr(n1) != nil {
		return b.seterror("wrong operand in call to AppEx %s(left: %d)", op, *n1)
	}
	if b.checkptr(n2) != nil {
		return b.seterror("wrong operand in call to AppEx %s(right: %d)", op, *n2)
	}
	if err := b.quantset2cache(*varset); err != nil {
		return nil
	}

	b.applycache.op = int(OPor)
	b.appexcache.op = int(op)
	b.appexcache.id = (*varset << 2) | b.appexcache.op
	b.quantcache.id = (b.appexcache.id << 3) | cacheidAPPEX
	b.initref()
	b.pushref(*n1)
	b.pushref(*n2)
	b.pushref(*varset)
	res := b.appquant(*n1, *n2, *varset)
	b.popref(3)
	return b.retnode(res)
}

func (b *BDD) appquant(left, right, varset int) int {
	switch Operator(b.appexcache.op) {
	case OPand:
		if left == 0 || right == 0 {
			return 0
		}
		if left == right {
			return b.quant(left, varset)
		}
		if left == 1 {
			return b.quant(right, varset)
		}
		if right == 1 {
			return b.quant(left, varset)
		}
	case OPor:
		if left == 1 || right == 1 {
			return 1
		}
		if left == right {
			return b.quant(left, varset)
		}
		if left == 0 {
			return b.quant(right, varset)
		}
		if right == 0 {
			return b.quant(left, varset)
		}
	case OPxor:
		if left == right {
			return 0
		}
		if left == 0 {
			return b.quant(right, varset)
		}
		if right == 0 {
			return b.quant(left, varset)
		}
	case OPnand:
		if left == 0 || right == 0 {
			return 1
		}
	case OPnor:
		if left == 1 || right == 1 {
			return 0
		}
	default:
		// OPnot and OPsimplify should not be used in apply.
		//
		// FIXME: we are raising an error for other operations that would be OK.
		b.seterror("unauthorized operation (%s) in AppEx", b.applycache.op)
		return -1
	}

	// we check for errors
	if left < 0 || right < 0 {
		b.seterror("unexpected error in appquant")
		return -1
	}

	// we deal with the other cases when the two operands are constants
	if (left < 2) && (right < 2) {
		return opres[b.appexcache.op][left][right]
	}

	// and the case where we have no more variables to quantify
	if (b.level(left) > b.quantlast) && (b.level(right) > b.quantlast) {
		oldop := b.applycache.op
		b.applycache.op = b.appexcache.op
		res := b.apply(left, right)
		b.applycache.op = oldop
		return res
	}

	// next we check if the operation is already in our cache
	if res := b.matchappex(left, right); res >= 0 {
		return res
	}
	leftlvl := b.level(left)
	rightlvl := b.level(right)
	var res int
	if leftlvl == rightlvl {
		low := b.pushref(b.appquant(b.low(left), b.low(right), varset))
		high := b.pushref(b.appquant(b.high(left), b.high(right), varset))
		if b.quantset[leftlvl] == b.quantsetID {
			res = b.apply(low, high)
		} else {
			res = b.makenode(leftlvl, low, high)
		}
	} else {
		if leftlvl < rightlvl {
			low := b.pushref(b.appquant(b.low(left), right, varset))
			high := b.pushref(b.appquant(b.high(left), right, varset))
			if b.quantset[leftlvl] == b.quantsetID {
				res = b.apply(low, high)
			} else {
				res = b.makenode(leftlvl, low, high)
			}
		} else {
			low := b.pushref(b.appquant(left, b.low(right), varset))
			high := b.pushref(b.appquant(left, b.high(right), varset))
			if b.quantset[rightlvl] == b.quantsetID {
				res = b.apply(low, high)
			} else {
				res = b.makenode(rightlvl, low, high)
			}
		}
	}
	b.popref(2)
	return b.setappex(left, right, res)
}

// Replace takes a Replacer and computes the result of n after replacing old
// variables with new ones. See type Replacer.
func (b *BDD) Replace(n Node, r Replacer) Node {
	if b.checkptr(n) != nil {
		return b.seterror("wrong operand in call to Replace (%d)", *n)
	}
	b.initref()
	b.pushref(*n)
	b.replacecache.id = r.Id()
	res := b.retnode(b.replace(*n, r))
	b.popref(1)
	return res
}

func (b *BDD) replace(n int, r Replacer) int {
	image, ok := r.Replace(b.level(n))
	if !ok {
		return n
	}
	if res := b.matchreplace(n); res >= 0 {
		return res
	}
	low := b.pushref(b.replace(b.low(n), r))
	high := b.pushref(b.replace(b.high(n), r))
	res := b.correctify(image, low, high)
	b.popref(2)
	return b.setreplace(n, res)
}

func (b *BDD) correctify(level int32, low, high int) int {
	/* FIXME: we do not use the cache here */
	if (level < b.level(low)) && (level < b.level(high)) {
		return b.makenode(level, low, high)
	}

	if (level == b.level(low)) || (level == b.level(high)) {
		b.seterror("error in replace level (%d) == low (%d:%d) or high (%d:%d)", level, low, b.level(low), high, b.level(high))
		return -1
	}

	if b.level(low) == b.level(high) {
		left := b.pushref(b.correctify(level, b.low(low), b.low(high)))
		right := b.pushref(b.correctify(level, b.high(low), b.high(high)))
		res := b.makenode(b.level(low), left, right)
		b.popref(2)
		return res
	}

	if b.level(low) < b.level(high) {
		left := b.pushref(b.correctify(level, b.low(low), high))
		right := b.pushref(b.correctify(level, b.high(low), high))
		res := b.makenode(b.level(low), left, right)
		b.popref(2)
		return res
	}

	left := b.pushref(b.correctify(level, low, b.low(high)))
	right := b.pushref(b.correctify(level, low, b.high(high)))
	res := b.makenode(b.level(high), left, right)
	b.popref(2)
	return res
}

// Satcount computes the number of satisfying variable assignments for the
// function denoted by n. We return a result using arbitrary-precision
// arithmetic to avoid possible overflows. The result is zero (and we set the
// error flag of b) if there is an error.
func (b *BDD) Satcount(n Node) *big.Int {
	res := big.NewInt(0)
	if b.checkptr(n) != nil {
		b.seterror("Wrong operand in call to Satcount (%d)", *n)
		return res
	}
	// We compute 2^level with a bit shift 1 << level
	res.SetBit(res, int(b.level(*n)), 1)
	satc := make(map[int]*big.Int)
	return res.Mul(res, b.satcount(*n, satc))
}

func (b *BDD) satcount(n int, satc map[int]*big.Int) *big.Int {
	if n < 2 {
		return big.NewInt(int64(n))
	}
	// we use satc to memoize the value of satcount for each nodes
	res, ok := satc[n]
	if ok {
		return res
	}
	level := b.level(n)
	low := b.low(n)
	high := b.high(n)

	res = big.NewInt(0)
	two := big.NewInt(0)
	two.SetBit(two, int(b.level(low)-level-1), 1)
	res.Add(res, two.Mul(two, b.satcount(low, satc)))
	two = big.NewInt(0)
	two.SetBit(two, int(b.level(high)-level-1), 1)
	res.Add(res, two.Mul(two, b.satcount(high, satc)))
	satc[n] = res
	return res
}

// Allsat Iterates through all legal variable assignments for n and calls the
// function f on each of them. We pass an int slice of length varnum to f where
// each entry is either  0 if the variable is false, 1 if it is true, and -1 if
// it is a don't care. We stop and return an error if f returns an error at some
// point.
func (b *BDD) Allsat(f func([]int) error, n Node) error {
	if b.checkptr(n) != nil {
		return fmt.Errorf("wrong node in call to Allsat (%d)", *n)
	}
	prof := make([]int, b.varnum)
	for k := range prof {
		prof[k] = -1
	}
	// the function does not create new nodes, so we do not need to take care of
	// possible resizing
	return b.allsat(*n, prof, f)
}

func (b *BDD) allsat(n int, prof []int, f func([]int) error) error {
	if n == 1 {
		return f(prof)
	}
	if n == 0 {
		return nil
	}

	if low := b.low(n); low != 0 {
		prof[b.level(n)] = 0
		for v := b.level(low) - 1; v > b.level(n); v-- {
			prof[v] = -1
		}
		if err := b.allsat(low, prof, f); err != nil {
			return nil
		}
	}

	if high := b.high(n); high != 0 {
		prof[b.level(n)] = 1
		for v := b.level(high) - 1; v > b.level(n); v-- {
			prof[v] = -1
		}
		if err := b.allsat(high, prof, f); err != nil {
			return nil
		}
	}
	return nil
}

// Allnodes applies function f over all the nodes accessible from the nodes in
// the sequence n..., or all the active nodes if n is absent (len(n) == 0). The
// parameters to function f are the id, level, and id's of the low and high
// successors of each node. The two constant nodes (True and False) have always
// the id 1 and 0, respectively. The order in which nodes are visited is not
// specified. The behavior is very similar to the one of Allsat. In particular,
// we stop the computation and return an error if f returns an error at some
// point.
func (b *BDD) Allnodes(f func(id, level, low, high int) error, n ...Node) error {
	for _, v := range n {
		if err := b.checkptr(v); err != nil {
			return fmt.Errorf("wrong node in call to Allnodes; %s", err)
		}
	}
	// the function does not create new nodes, so we do not need to take care of
	// possible resizing.
	if len(n) == 0 {
		// we call f over all active nodes
		return b.allnodes(f)
	}
	return b.allnodesfrom(f, n)
}
