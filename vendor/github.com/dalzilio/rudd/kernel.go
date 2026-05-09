// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

import (
	"errors"
)

// number of bytes in a int (adapted from uintSize in the math/bits package)
const huddsize = (2*(32<<(^uint(0)>>32&1)) + 32) / 8 // 12 (32 bits) or 20 (64 bits)

// _MINFREENODES is the minimal number of nodes (%) that has to be left after a
// garbage collect unless a resize should be done.
const _MINFREENODES int = 20

// _MAXVAR is the maximal number of levels in the BDD. We use only the first 21
// bits for encoding levels (so also the max number of variables). We use 11
// other bits for markings. Hence we make sure to always use int32 to avoid
// problem when we change architecture.
const _MAXVAR int32 = 0x1FFFFF

// _MAXREFCOUNT is the maximal value of the reference counter (refcou), also
// used to stick nodes (like constants and variables) in the node list. It is
// egal to 1023 (10 bits).
const _MAXREFCOUNT int32 = 0x3FF

// _DEFAULTMAXNODEINC is the default value for the maximal increase in the
// number of nodes during a resize. It is approx. one million nodes (1 048 576)
// (could be interesting to change it to 1 << 23 = 8 388 608).
const _DEFAULTMAXNODEINC int = 1 << 20

var errMemory = errors.New("unable to free memory or resize BDD")
var errResize = errors.New("should cache resize") // when gbc and then noderesize
var errReset = errors.New("should cache reset")   // when gbc only, without resizing
