// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

import (
	"fmt"
)

// // BDD is an interface implementing the basic operations over Binary Decision
// // Diagrams.
// type BDD interface {
// 	// Error returns the error status of the BDD. We return an empty string if
// 	// there are no errors. Functions that return a Node result will signal an
// 	// error by returning nil.
// 	Error() string

// 	// Varnum returns the number of defined variables.
// 	Varnum() int

// 	// Ithvar returns a BDD representing the i'th variable on success. The
// 	// requested variable must be in the range [0..Varnum).
// 	Ithvar(i int) Node

// 	// NIthvar returns a bdd representing the negation of the i'th variable on
// 	// success. See *ithvar* for further info.
// 	NIthvar(i int) Node

// 	// Low returns the false branch of a BDD or nil if there is an error.
// 	Low(n Node) Node

// 	// High returns the true branch of a BDD.
// 	High(n Node) Node

// 	// Makeset returns a node corresponding to the conjunction (the cube) of all
// 	// the variables in varset, in their positive form. It is such that
// 	// scanset(Makeset(a)) == a. It returns nil if one of the variables is
// 	// outside the scope of the BDD (see documentation for function *Ithvar*).
// 	Makeset(varset []int) Node

// 	// Scanset returns the set of variables found when following the high branch
// 	// of node n. This is the dual of function Makeset. The result may be nil if
// 	// there is an error and it is an empty slice if the set is empty.
// 	Scanset(n Node) []int

// 	// Not returns the negation (!n) of expression n.
// 	Not(n Node) Node

// 	// Apply performs all of the basic binary operations on BDD nodes, such as
// 	// AND, OR etc.
// 	Apply(left Node, right Node, op Operator) Node

// 	// Ite, short for if-then-else operator, computes the BDD for the expression
// 	// [(f &  g) | (!f & h)] more efficiently than doing the three operations
// 	// separately.
// 	Ite(f, g, h Node) Node

// 	// Exist returns the existential quantification of n for the variables in
// 	// varset, where varset is a node built with a method such as Makeset.
// 	Exist(n, varset Node) Node

// 	// AppEx applies the binary operator *op* on the two operands left and right
// 	// then performs an existential quantification over the variables in varset,
// 	// where varset is a node computed with an operation such as Makeset.
// 	AppEx(left Node, right Node, op Operator, varset Node) Node

// 	// Replace takes a renamer and computes the result of n after replacing old
// 	// variables with new ones. See type Renamer.
// 	Replace(n Node, r Replacer) Node

// 	// Satcount computes the number of satisfying variable assignments for the
// 	// function denoted by n. We return a result using arbitrary-precision
// 	// arithmetic to avoid possible overflows. The result is zero (and we set
// 	// the error flag of b) if there is an error.
// 	Satcount(n Node) *big.Int

// 	// Allsat Iterates through all legal variable assignments for n and calls
// 	// the function f on each of them. We pass an int slice of length varnum to
// 	// f where each entry is either 0 if the variable is false, 1 if it is true,
// 	// and -1 if it is a don't care. We stop and return an error if f returns an
// 	// error at some point.
// 	Allsat(n Node, f func([]int) error) error

// 	// Allnodes is similar to Allsat but iterates over all the nodes accessible
// 	// from one of the parameters in n (or all the active nodes if n is absent).
// 	// Function f takes the id, level, and id's of the low and high successors
// 	// of each node. The two constant nodes (True and False) have always the id
// 	// 1 and 0 respectively.
// 	Allnodes(f func(id, level, low, high int) error, n ...Node) error

// 	// Stats returns information about the BDD
// 	Stats() string
// }

// // implementation is an unexported interface implemented by different BDD
// // structures
// type implementation interface {
// 	// retnode creates a Node for external use and sets a finalizer on it so
// 	// that the runtime can reclaim the ressource during GC.
// 	retnode(n int) Node

// 	// makenode is the only method for inserting a new BDD node
// 	makenode(level int32, low, high int, refstack []int) (int, error)

// 	// size returns the allocated size for the node table
// 	size() int

// 	// level return s the level of a node
// 	level(n int) int32

// 	low(n int) int

// 	high(n int) int

// 	ismarked(n int) bool

// 	marknode(n int)

// 	unmarknode(n int)

// 	// allnodes applies function f over all the nodes accessible in a node table
// 	allnodes(f func(id, level, low, high int) error) error

// 	// allnodesfrom applies function f over all the nodes accessible from the nodes in n
// 	allnodesfrom(f func(id, level, low, high int) error, n []Node) error

// 	// stats return a description of the state of the node table implementation
// 	stats() string
// }

// Node is a reference to an element of a BDD. It represents the atomic unit of
// interactions and computations within a BDD.
type Node *int

// inode returns a Node for known nodes, such as variables, that do not need to
// increase their reference count.
func inode(n int) Node {
	x := n
	return &x
}

var bddone Node = inode(1)

var bddzero Node = inode(0)

// BDD is the type of Binary Decision Diagrams. It abstracts and encapsulates
// the internal states of a BDD; such as caches, or the internal node and
// unicity tables for example. We propose multiple implementations (two at the
// moment) all based on approaches where we use integers as the key for Nodes.
type BDD struct {
	varnum   int32    // Number of BDD variables.
	varset   [][2]int // Set of variables used for Ithvar and NIthvar: we have a pair for each variable for its positive and negative occurrence
	refstack []int    // Internal node reference stack, used to avoid collecting nodes while they are being processed.
	error             // Error status: we use nil Nodes to signal a problem and store the error in this field. This help chain operations together.
	caches            // Set of caches used for the operations in the BDD
	*tables           // Underlying struct that encapsulates the list of nodes
}

// Varnum returns the number of defined variables.
func (b *BDD) Varnum() int {
	return int(b.varnum)
}

func (b *BDD) makenode(level int32, low, high int) int {
	res, err := b.tables.makenode(level, low, high, b.refstack)
	if err == nil {
		return res
	}
	if err == errReset {
		b.cachereset()
		return res
	}
	if err == errResize {
		b.cacheresize(b.size())
		return res
	}
	return res
}

// caches is a collection of caches used for operations
type caches struct {
	*applycache   // Cache for apply results
	*itecache     // Cache for ITE results
	*quantcache   // Cache for exist/forall results
	*appexcache   // Cache for AppEx results
	*replacecache // Cache for Replace results
}

// initref is part of three private functions to manipulate the refstack; used
// to prevent nodes that are currently being built (e.g. transient nodes built
// during an apply) to be reclaimed during GC.
func (b *BDD) initref() {
	b.refstack = b.refstack[:0]
}

func (b *BDD) pushref(n int) int {
	b.refstack = append(b.refstack, n)
	return n
}

func (b *BDD) popref(a int) {
	b.refstack = b.refstack[:len(b.refstack)-a]
}

// gcstat stores status information about garbage collections. We use a stack
// (slice) of objects to record the sequence of GC during a computation.
type gcstat struct {
	setfinalizers    uint64    // Total number of external references to BDD nodes
	calledfinalizers uint64    // Number of external references that were freed
	history          []gcpoint // Snaphot of GC stats at each occurrence
}

type gcpoint struct {
	nodes            int // Total number of allocated nodes in the nodetable
	freenodes        int // Number of free nodes in the nodetable
	setfinalizers    int // Total number of external references to BDD nodes
	calledfinalizers int // Number of external references that were freed
}

// checkptr performs a sanity check prior to accessing a node and return eventual
// error code.
func (b *BDD) checkptr(n Node) error {
	switch {
	case n == nil:
		b.seterror("Illegal acces to node (nil value)")
		return b.error
	case (*n < 0) || (*n >= b.size()):
		b.seterror("Illegal acces to node %d", *n)
		return b.error
	case (*n >= 2) && (b.low(*n) == -1):
		b.seterror("Illegal acces to node %d", *n)
		return b.error
	}
	return nil
}

// Ithvar returns a BDD representing the i'th variable on success (the
// expression xi), otherwise we set the error status in the BDD and returns the
// nil node. The requested variable must be in the range [0..Varnum).
func (b *BDD) Ithvar(i int) Node {
	if (i < 0) || (int32(i) >= b.varnum) {
		return b.seterror("Unknown variable used (%d) in call to ithvar", i)
	}
	// we do not need to reference count variables
	return inode(b.varset[i][0])
}

// NIthvar returns a node representing the negation of the i'th variable on
// success (the expression !xi), otherwise the nil node. See *ithvar* for
// further info.
func (b *BDD) NIthvar(i int) Node {
	if (i < 0) || (int32(i) >= b.varnum) {
		return b.seterror("Unknown variable used (%d) in call to nithvar", i)
	}
	// we do not need to reference count variables
	return inode(b.varset[i][1])
}

// Label returns the variable (index) corresponding to node n in the BDD. We set
// the BDD to its error state and return -1 if we try to access a constant node.
func (b *BDD) Label(n Node) int {
	if b.checkptr(n) != nil {
		b.seterror("Illegal access to node %d in call to Label", n)
		return -1
	}
	if *n < 2 {
		b.seterror("Try to access label of constant node")
		return -1
	}
	return int(b.level(*n))
}

// Low returns the false (or low) branch of a BDD. We return nil and set the
// error flag in the BDD if there is an error.
func (b *BDD) Low(n Node) Node {
	if b.checkptr(n) != nil {
		return b.seterror("Illegal access to node %d in call to Low", n)
	}
	return b.retnode(b.low(*n))
}

// High returns the true (or high) branch of a BDD. We return nil and set the
// error flag in the BDD if there is an error.
func (b *BDD) High(n Node) Node {
	if b.checkptr(n) != nil {
		return b.seterror("Illegal access to node %d in call to High", n)
	}
	return b.retnode(b.high(*n))
}

// And returns the logical 'and' of a sequence of nodes or, equivalently,
// computes the intersection of a sequence of Boolean vectors.
func (b *BDD) And(n ...Node) Node {
	if len(n) == 1 {
		return n[0]
	}
	if len(n) == 0 {
		return bddone
	}
	return b.Apply(n[0], b.And(n[1:]...), OPand)
}

// Or returns the logical 'or' of a sequence of nodes or, equivalently, computes
// the union of a sequence of Boolean vectors.
func (b *BDD) Or(n ...Node) Node {
	if len(n) == 1 {
		return n[0]
	}
	if len(n) == 0 {
		return bddzero
	}
	return b.Apply(n[0], b.Or(n[1:]...), OPor)
}

// Imp returns the logical 'implication' between two BDDs.
func (b *BDD) Imp(n1, n2 Node) Node {
	return b.Apply(n1, n2, OPimp)
}

// Equiv returns the logical 'bi-implication' between two BDDs.
func (b *BDD) Equiv(n1, n2 Node) Node {
	return b.Apply(n1, n2, OPbiimp)
}

// Equal tests equivalence between nodes.
func (b *BDD) Equal(n1, n2 Node) bool {
	if n1 == n2 {
		return true
	}
	if n1 == nil || n2 == nil {
		return false
	}
	return *n1 == *n2
}

// AndExist returns the "relational composition" of two nodes with respect to
// varset, meaning the result of (∃ varset . n1 & n2).
func (b *BDD) AndExist(varset, n1, n2 Node) Node {
	return b.AppEx(n1, n2, OPand, varset)
}

// True returns the constant true BDD (a node pointing to the value 1). Our
// implementation ensures that this pointer is unique. Hence two successive call
// to True should return the same node.
func (b *BDD) True() Node {
	return bddone
}

// False returns the constant false BDD (a node pointing to the value 0).
func (b *BDD) False() Node {
	return bddzero
}

// From returns a (constant) Node from a boolean value. We return the (BDD)
// value True if v is true and False otherwise.
func (b *BDD) From(v bool) Node {
	if v {
		return bddone
	}
	return bddzero
}

// Stats returns information about the BDD. It is possible to print more
// information about the caches and memory footprint of the BDD by compiling
// your executable with the build tag 'debug'.
func (b *BDD) Stats() string {
	res := "==============\n"
	res += fmt.Sprintf("Varnum:     %d\n", b.varnum)
	res += b.stats()
	if _DEBUG {
		res += "==============\n"
		res += b.applycache.String()
		res += b.itecache.String()
		res += b.quantcache.String()
		res += b.appexcache.String()
		res += b.replacecache.String()
	}
	return res
}
