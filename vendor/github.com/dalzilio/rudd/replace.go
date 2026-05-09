// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

import (
	"fmt"
	"math"
)

var _REPLACEID = 1

// Replacer is the types of substitution objects used in a Replace operation,
// that substitutes variables in a BDD "function". The only method returning an
// object of this type is the NewReplacer method. The result obtained when using
// a replacer created from a BDD, in a Replace operation over a different BDD is
// unspecified.
type Replacer interface {
	Replace(int32) (int32, bool)
	Id() int
}

type replacer struct {
	id    int     // unique identifier used for caching intermediate results
	image []int32 // map the level of old variables to the level of new variables
	last  int32   // last index in the Replacer, to speed up computations
}

func (r *replacer) String() string {
	res := fmt.Sprintf("replacer(last: %d)[", r.last)
	first := true
	for k, v := range r.image {
		if k != int(v) {
			if !first {
				res += ", "
			}
			first = false
			res += fmt.Sprintf("%d<-%d", k, v)
		}
	}
	return res + "]"
}

func (r *replacer) Replace(level int32) (int32, bool) {
	if level > r.last {
		return level, false
	}
	return r.image[level], true
}

func (r *replacer) Id() int {
	return r.id
}

// NewReplacer returns a Replacer that can be used for substituting variable
// oldvars[k] with newvars[k] in the BDD b. We return an error if the two slices
// do not have the same length or if we find the same index twice in either of
// them. All values must be in the interval [0..Varnum).
func (b *BDD) NewReplacer(oldvars, newvars []int) (Replacer, error) {
	res := &replacer{}
	if len(oldvars) != len(newvars) {
		return nil, fmt.Errorf("unmatched length of slices")
	}
	if _REPLACEID == (math.MaxInt32 >> 2) {
		return nil, fmt.Errorf("too many replacers created")
	}
	res.id = (_REPLACEID << 2) | cacheidREPLACE
	_REPLACEID++
	varnum := b.Varnum()
	support := make([]bool, varnum)
	res.image = make([]int32, varnum)
	for k := range res.image {
		res.image[k] = int32(k)
	}
	for k, v := range oldvars {
		if support[v] {
			return nil, fmt.Errorf("duplicate variable (%d) in oldvars", v)
		}
		if v >= varnum {
			return nil, fmt.Errorf("invalid variable in oldvars (%d)", v)
		}
		if newvars[k] >= varnum {
			return nil, fmt.Errorf("invalid variable in newvars (%d)", v)
		}
		support[v] = true
		res.image[v] = int32(newvars[k])
		if int32(v) > res.last {
			res.last = int32(v)
		}
	}
	for _, v := range newvars {
		if int(res.image[v]) != v {
			return nil, fmt.Errorf("variable in newvars (%d) also occur in oldvars", v)
		}
	}
	return res, nil
}
