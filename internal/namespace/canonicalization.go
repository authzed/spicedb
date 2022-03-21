package namespace

import (
	"fmt"
	"hash/fnv"

	"github.com/dalzilio/rudd"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

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
//   definition somenamespace {
//	    relation first: ...
//      relation second: ...
//      permission someperm = second + (first - third->something)
//   }
//
// We begin by assigning a unique integer index to each relation and arrow found for all
// expressions in the namespace:
//   definition somenamespace {
//	    relation first: ...
//               ^ index 0
//      relation second: ...
//               ^ index 1
//      permission someperm = second + (first - third->something)
//                            ^ 1       ^ 0     ^ index 2
//   }
//
// These indexes are then used with the rudd library to build the expression:
//    someperm => `bdd.And(bdd.Ithvar(1), bdd.Or(bdd.Ithvar(0), bdd.NIthvar(2)))`
//
// The `rudd` library automatically handles associativity, and produces a hash representing the
// canonical representation of the binary expression. These hashes can then be used for caching,
// representing the same *logical* expressions for a permission, even if the relations have
// different names.
func computeCanonicalCacheKeys(typeSystem *ValidatedNamespaceTypeSystem, aliasMap map[string]string) (map[string]string, error) {
	varMap := buildBddVarMap(typeSystem.nsDef.Relation, aliasMap)
	if varMap.Len() == 0 {
		return map[string]string{}, nil
	}

	bdd, err := rudd.New(varMap.Len())
	if err != nil {
		return nil, err
	}

	// For each permission, build a canonicalized cache key based on its expression.
	cacheKeys := make(map[string]string, len(typeSystem.nsDef.Relation))
	for _, rel := range typeSystem.nsDef.Relation {
		rewrite := rel.GetUsersetRewrite()
		if rewrite == nil {
			continue
		}

		hasher := fnv.New64a()
		bdd.Print(hasher, convertRewriteToBdd(rel, bdd, rewrite, varMap))
		cacheKeys[rel.Name] = fmt.Sprintf("%x", hasher.Sum64())
	}

	return cacheKeys, nil
}

func convertRewriteToBdd(relation *core.Relation, bdd *rudd.BDD, rewrite *core.UsersetRewrite, varMap bddVarMap) rudd.Node {
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
		return convertToBdd(relation, bdd, rw.Exclusion, bdd.Or, func(childIndex int, varIndex int) rudd.Node {
			if childIndex == 0 {
				return bdd.Ithvar(varIndex)
			}
			return bdd.NIthvar(varIndex)
		}, varMap)

	default:
		panic(fmt.Sprintf("Unknown rewrite kind %v", rw))
	}
}

type (
	combiner func(n ...rudd.Node) rudd.Node
	builder  func(childIndex int, varIndex int) rudd.Node
)

func convertToBdd(relation *core.Relation, bdd *rudd.BDD, so *core.SetOperation, combiner combiner, builder builder, varMap bddVarMap) rudd.Node {
	values := make([]rudd.Node, 0, len(so.Child))
	for index, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			// TODO(jschorr): Turn into an error once v0 API has been removed.
			log.Warn().Stringer("operation", so).Msg("Use of _this is deprecated and will soon be an error! Please switch to using schema!")
			values = append(values, builder(index, varMap.Get(relation.Name)))
		case *core.SetOperation_Child_ComputedUserset:
			values = append(values, builder(index, varMap.Get(child.ComputedUserset.Relation)))
		case *core.SetOperation_Child_UsersetRewrite:
			values = append(values, convertRewriteToBdd(relation, bdd, child.UsersetRewrite, varMap))
		case *core.SetOperation_Child_TupleToUserset:
			values = append(values, builder(index, varMap.GetArrow(child.TupleToUserset.Tupleset.Relation, child.TupleToUserset.ComputedUserset.Relation)))
		}
	}
	return combiner(values...)
}

type bddVarMap struct {
	aliasMap map[string]string
	varMap   map[string]int
}

func (bvm bddVarMap) GetArrow(tuplesetName string, relName string) int {
	key := fmt.Sprintf("%s->%s", tuplesetName, relName)
	index, ok := bvm.varMap[key]
	if !ok {
		panic(fmt.Sprintf("Missing arrow key %s in varMap", key))
	}
	return index
}

func (bvm bddVarMap) Get(relName string) int {
	if alias, ok := bvm.aliasMap[relName]; ok {
		return bvm.Get(alias)
	}

	index, ok := bvm.varMap[relName]
	if !ok {
		panic(fmt.Sprintf("Missing key %s in varMap", relName))
	}
	return index
}

func (bvm bddVarMap) Len() int {
	return len(bvm.varMap)
}

func buildBddVarMap(relations []*core.Relation, aliasMap map[string]string) bddVarMap {
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

		graph.WalkRewrite(rewrite, func(childOneof *core.SetOperation_Child) interface{} {
			switch child := childOneof.ChildType.(type) {
			case *core.SetOperation_Child_TupleToUserset:
				key := fmt.Sprintf("%s->%s", child.TupleToUserset.Tupleset.Relation, child.TupleToUserset.ComputedUserset.Relation)
				if _, ok := varMap[key]; !ok {
					varMap[key] = len(varMap)
				}
			}
			return nil
		})
	}
	return bddVarMap{
		aliasMap: aliasMap,
		varMap:   varMap,
	}
}
