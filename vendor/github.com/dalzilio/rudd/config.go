// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

// configs is used to store the values of different parameters of the BDD
type configs struct {
	varnum          int // number of BDD variables
	nodesize        int // initial number of nodes in the table
	cachesize       int // initial cache size (general)
	cacheratio      int // initial ratio (general, 0 if size constant) between cache size and node table
	maxnodesize     int // Maximum total number of nodes (0 if no limit)
	maxnodeincrease int // Maximum number of nodes that can be added to the table at each resize (0 if no limit)
	minfreenodes    int // Minimum number of nodes that should be left after GC before triggering a resize
}

func makeconfigs(varnum int) *configs {
	c := &configs{varnum: varnum}
	c.minfreenodes = _MINFREENODES
	c.maxnodeincrease = _DEFAULTMAXNODEINC
	// we build enough nodes to include all the variables in varset
	c.nodesize = 2*varnum + 2
	return c
}

// Nodesize is a configuration option (function). Used as a parameter in New it
// sets a preferred initial size for the node table. The size of the BDD can
// increase during computation. By default we create a table large enough to
// include the two constants and the "variables" used in the call to Ithvar and
// NIthvar.
func Nodesize(size int) func(*configs) {
	return func(c *configs) {
		if size >= 2*c.varnum+2 {
			c.nodesize = size
		}
	}
}

// Maxnodesize is a configuration option (function). Used as a parameter in New
// it sets a limit to the number of nodes in the BDD. An operation trying to
// raise the number of nodes above this limit will generate an error and return
// a nil Node. The default value (0) means that there is no limit. In which case
// allocation can panic if we exhaust all the available memory.
func Maxnodesize(size int) func(*configs) {
	return func(c *configs) {
		c.maxnodesize = size
	}
}

// Maxnodeincrease is a configuration option (function). Used as a parameter in
// New it sets a limit on the increase in size of the node table. Below this
// limit we typically double the size of the node list each time we need to
// resize it. The default value is about a million nodes. Set the value to zero
// to avoid imposing a limit.
func Maxnodeincrease(size int) func(*configs) {
	return func(c *configs) {
		c.maxnodeincrease = size
	}
}

// Minfreenodes is a configuration option (function). Used as a parameter in New
// it sets the ratio of free nodes (%) that has to be left after a Garbage
// Collection event. When there is not enough free nodes in the BDD, we try
// reclaiming unused nodes. With a ratio of, say 25, we resize the table if the
// number a free nodes is less than 25% of the capacity of the table (see
// Maxnodesize and Maxnodeincrease). The default value is 20%.
func Minfreenodes(ratio int) func(*configs) {
	return func(c *configs) {
		c.minfreenodes = ratio
	}
}

// Cachesize is a configuration option (function). Used as a parameter in New it
// sets the initial number of entries in the operation caches. The default value
// is 10 000. Typical values for nodesize are 10 000 nodes for small test
// examples and up to 1 000 000 nodes for large examples. See also the
// Cacheratio config.
func Cachesize(size int) func(*configs) {
	return func(c *configs) {
		c.cachesize = size
	}
}

// Cacheratio is a configuration option (function). Used as a parameter in New
// it sets a "cache ratio" (%) so that caches can grow each time we resize the
// node table. With a cache ratio of r, we have r available entries in the cache
// for every 100 slots in the node table. (A typical value for the cache ratio
// is 25% or 20%). The default value (0) means that the cache size never grows.
func Cacheratio(ratio int) func(*configs) {
	return func(c *configs) {
		c.cacheratio = ratio
	}
}
