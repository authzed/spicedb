package namespace

import (
	"encoding/hex"
	"hash/fnv"

	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"

	"github.com/dalzilio/rudd"

	"github.com/authzed/spicedb/pkg/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const computedKeyPrefix = "%"

// computeCanonicalCacheKeys computes a map from permission name to associated canonicalized
// cache key for each non-aliased permission in the given type system's namespace.
//
// Canonicalization works by taking each permission's userset rewrite expression and transforming
// it into a Binary Decision Diagram (BDD) via the `rudd` library.
//
// Each access of a relation or arrow is assigned a unique integer ID within the *namespace*,
// and the operations (+, -, &) are converted into binary operations.
//
// For example, for the namespace:
//
//	definition somenamespace {
//	   relation first: ...
//	   relation second: ...
//	   relation third: ...
//	   permission someperm = second + (first - third->something)
//	}
//
// We begin by assigning a unique integer index to each relation and arrow found for all
// expressions in the namespace:
//
//	  definition somenamespace {
//		 relation first: ...
//	              ^ index 0
//	     relation second: ...
//	              ^ index 1
//	     relation third: ...
//	              ^ index 2
//	     permission someperm = second + (first - third->something)
//	                           ^ 1       ^ 0     ^ index 3
//	  }
//
// These indexes are then used with the rudd library to build the expression:
//
//	someperm => `bdd.Or(bdd.Ithvar(1), bdd.And(bdd.Ithvar(0), bdd.NIthvar(2)))`
//
// The `rudd` library automatically handles associativity, and produces a hash representing the
// canonical representation of the binary expression. These hashes can then be used for caching,
// representing the same *logical* expressions for a permission, even if the relations have
// different names.
func computeCanonicalCacheKeys(typeDef *schema.ValidatedDefinition, aliasMap map[string]string) (map[string]string, error) {
	varMap, err := buildBddVarMap(typeDef.Namespace().Relation, aliasMap)
	if err != nil {
		return nil, err
	}

	if varMap.Len() == 0 {
		return map[string]string{}, nil
	}

	bdd, err := rudd.New(varMap.Len())
	if err != nil {
		return nil, err
	}

	// For each permission, build a canonicalized cache key based on its expression.
	cacheKeys := make(map[string]string, len(typeDef.Namespace().Relation))
	for _, rel := range typeDef.Namespace().Relation {
		rewrite := rel.GetUsersetRewrite()
		if rewrite == nil {
			// If the relation has no rewrite (making it a pure relation), then its canonical
			// key is simply the relation's name.
			cacheKeys[rel.Name] = rel.Name
			continue
		}

		hasher := fnv.New64a()
		node, err := convertRewriteToBdd(rel, bdd, rewrite, varMap)
		if err != nil {
			return nil, err
		}

		bdd.Print(hasher, node)
		cacheKeys[rel.Name] = computedKeyPrefix + hex.EncodeToString(hasher.Sum(nil))
	}

	return cacheKeys, nil
}

func convertRewriteToBdd(relation *core.Relation, bdd *rudd.BDD, rewrite *core.UsersetRewrite, varMap bddVarMap) (rudd.Node, error) {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return convertToBdd(relation, bdd, rw.Union, bdd.Or, func(childIndex int, varIndex int) rudd.Node {
			return bdd.Ithvar(varIndex)
		}, varMap)

	case *core.UsersetRewrite_Intersection:
		return convertToBdd(relation, bdd, rw.Intersection, bdd.And, func(childIndex int, varIndex int) rudd.Node {
			return bdd.Ithvar(varIndex)
		}, varMap)

	case *core.UsersetRewrite_Exclusion:
		return convertToBdd(relation, bdd, rw.Exclusion, bdd.And, func(childIndex int, varIndex int) rudd.Node {
			if childIndex == 0 {
				return bdd.Ithvar(varIndex)
			}
			return bdd.NIthvar(varIndex)
		}, varMap)

	default:
		return nil, spiceerrors.MustBugf("Unknown rewrite kind %v", rw)
	}
}

type (
	combiner func(n ...rudd.Node) rudd.Node
	builder  func(childIndex int, varIndex int) rudd.Node
)

func convertToBdd(relation *core.Relation, bdd *rudd.BDD, so *core.SetOperation, combiner combiner, builder builder, varMap bddVarMap) (rudd.Node, error) {
	values := make([]rudd.Node, 0, len(so.Child))
	for index, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return nil, spiceerrors.MustBugf("use of _this is disallowed")

		case *core.SetOperation_Child_ComputedUserset:
			cuIndex, err := varMap.Get(child.ComputedUserset.Relation)
			if err != nil {
				return nil, err
			}

			values = append(values, builder(index, cuIndex))

		case *core.SetOperation_Child_UsersetRewrite:
			node, err := convertRewriteToBdd(relation, bdd, child.UsersetRewrite, varMap)
			if err != nil {
				return nil, err
			}

			values = append(values, node)

		case *core.SetOperation_Child_TupleToUserset:
			arrowIndex, err := varMap.GetArrow(child.TupleToUserset.Tupleset.Relation, child.TupleToUserset.ComputedUserset.Relation)
			if err != nil {
				return nil, err
			}

			values = append(values, builder(index, arrowIndex))

		case *core.SetOperation_Child_FunctionedTupleToUserset:
			switch child.FunctionedTupleToUserset.Function {
			case core.FunctionedTupleToUserset_FUNCTION_ANY:
				arrowIndex, err := varMap.GetArrow(child.FunctionedTupleToUserset.Tupleset.Relation, child.FunctionedTupleToUserset.ComputedUserset.Relation)
				if err != nil {
					return nil, err
				}

				values = append(values, builder(index, arrowIndex))

			case core.FunctionedTupleToUserset_FUNCTION_ALL:
				arrowIndex, err := varMap.GetIntersectionArrow(child.FunctionedTupleToUserset.Tupleset.Relation, child.FunctionedTupleToUserset.ComputedUserset.Relation)
				if err != nil {
					return nil, err
				}

				values = append(values, builder(index, arrowIndex))

			default:
				return nil, spiceerrors.MustBugf("unknown function %v", child.FunctionedTupleToUserset.Function)
			}

		case *core.SetOperation_Child_XNil:
			values = append(values, builder(index, varMap.Nil()))

		default:
			return nil, spiceerrors.MustBugf("unknown set operation child %T", child)
		}
	}
	return combiner(values...), nil
}

type bddVarMap struct {
	aliasMap map[string]string
	varMap   map[string]int
}

func (bvm bddVarMap) GetArrow(tuplesetName string, relName string) (int, error) {
	key := tuplesetName + "->" + relName
	index, ok := bvm.varMap[key]
	if !ok {
		return -1, spiceerrors.MustBugf("missing arrow key %s in varMap", key)
	}
	return index, nil
}

func (bvm bddVarMap) GetIntersectionArrow(tuplesetName string, relName string) (int, error) {
	key := tuplesetName + "-(all)->" + relName
	index, ok := bvm.varMap[key]
	if !ok {
		return -1, spiceerrors.MustBugf("missing intersection arrow key %s in varMap", key)
	}
	return index, nil
}

func (bvm bddVarMap) Nil() int {
	return len(bvm.varMap)
}

func (bvm bddVarMap) Get(relName string) (int, error) {
	if alias, ok := bvm.aliasMap[relName]; ok {
		return bvm.Get(alias)
	}

	index, ok := bvm.varMap[relName]
	if !ok {
		return -1, spiceerrors.MustBugf("missing key %s in varMap", relName)
	}
	return index, nil
}

func (bvm bddVarMap) Len() int {
	return len(bvm.varMap) + 1 // +1 for `nil`
}

func buildBddVarMap(relations []*core.Relation, aliasMap map[string]string) (bddVarMap, error) {
	varMap := map[string]int{}
	for _, rel := range relations {
		if _, ok := aliasMap[rel.Name]; ok {
			continue
		}

		varMap[rel.Name] = len(varMap)

		rewrite := rel.GetUsersetRewrite()
		if rewrite == nil {
			continue
		}

		_, err := graph.WalkRewrite(rewrite, func(childOneof *core.SetOperation_Child) (interface{}, error) {
			switch child := childOneof.ChildType.(type) {
			case *core.SetOperation_Child_TupleToUserset:
				key := child.TupleToUserset.Tupleset.Relation + "->" + child.TupleToUserset.ComputedUserset.Relation
				if _, ok := varMap[key]; !ok {
					varMap[key] = len(varMap)
				}
			case *core.SetOperation_Child_FunctionedTupleToUserset:
				key := child.FunctionedTupleToUserset.Tupleset.Relation + "->" + child.FunctionedTupleToUserset.ComputedUserset.Relation

				switch child.FunctionedTupleToUserset.Function {
				case core.FunctionedTupleToUserset_FUNCTION_ANY:
					// Use the key.

				case core.FunctionedTupleToUserset_FUNCTION_ALL:
					key = child.FunctionedTupleToUserset.Tupleset.Relation + "-(all)->" + child.FunctionedTupleToUserset.ComputedUserset.Relation

				default:
					return nil, spiceerrors.MustBugf("unknown function %v", child.FunctionedTupleToUserset.Function)
				}

				if _, ok := varMap[key]; !ok {
					varMap[key] = len(varMap)
				}
			}
			return nil, nil
		})
		if err != nil {
			return bddVarMap{}, err
		}
	}
	return bddVarMap{
		aliasMap: aliasMap,
		varMap:   varMap,
	}, nil
}
