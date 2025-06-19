package schema

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const ellipsesRelation = "..."

// GetRecursiveTerminalTypesForRelation returns, for a given definition and relation, all the potential
// terminal subject type definition names of that relation.
func (ts *TypeSystem) GetRecursiveTerminalTypesForRelation(ctx context.Context, defName string, relationName string) ([]string, error) {
	seen := mapz.NewSet[string]()
	set, err := ts.getTypesForRelationInternal(ctx, defName, relationName, seen, nil)
	if err != nil {
		return nil, err
	}
	if set == nil {
		return nil, nil
	}
	return set.AsSlice(), nil
}

// GetFullRecursiveSubjectTypesForRelation returns, for a given definition and relation, all the potential
// terminal subject type definition names of that relation, as well as any relation subtypes (eg, `group#member`) that may occur.
func (ts *TypeSystem) GetFullRecursiveSubjectTypesForRelation(ctx context.Context, defName string, relationName string) ([]string, error) {
	seen := mapz.NewSet[string]()
	nonTerminals := mapz.NewSet[string]()
	set, err := ts.getTypesForRelationInternal(ctx, defName, relationName, seen, nonTerminals)
	if err != nil {
		return nil, err
	}
	set.Merge(nonTerminals)
	return set.AsSlice(), nil
}

func (ts *TypeSystem) getTypesForRelationInternal(ctx context.Context, defName string, relationName string, seen *mapz.Set[string], nonTerminals *mapz.Set[string]) (*mapz.Set[string], error) {
	id := fmt.Sprint(defName, "#", relationName)
	if seen.Has(id) {
		return nil, nil
	}
	seen.Add(id)
	def, err := ts.GetDefinition(ctx, defName)
	if err != nil {
		return nil, err
	}
	rel, ok := def.GetRelation(relationName)
	if !ok {
		// Supress the error that it couldn't find the relation, as if a relation is missing in a definition, it's already a noop in dispatch.
		return nil, nil
	}
	if rel.TypeInformation != nil {
		return ts.getTypesForInfo(ctx, rel.TypeInformation, seen, nonTerminals)
	} else if rel.UsersetRewrite != nil {
		return ts.getTypesForRewrite(ctx, defName, rel.UsersetRewrite, seen, nonTerminals)
	}
	return nil, asTypeError(NewMissingAllowedRelationsErr(defName, relationName))
}

func (ts *TypeSystem) getTypesForInfo(ctx context.Context, rel *corev1.TypeInformation, seen *mapz.Set[string], nonTerminals *mapz.Set[string]) (*mapz.Set[string], error) {
	out := mapz.NewSet[string]()
	for _, dr := range rel.GetAllowedDirectRelations() {
		switch {
		case dr.GetRelation() == ellipsesRelation:
			out.Add(dr.GetNamespace())
		case dr.GetRelation() != "":
			if nonTerminals != nil {
				nonTerminals.Add(fmt.Sprintf("%s#%s", dr.GetNamespace(), dr.GetRelation()))
			}
			rest, err := ts.getTypesForRelationInternal(ctx, dr.GetNamespace(), dr.GetRelation(), seen, nonTerminals)
			if err != nil {
				return nil, err
			}
			if rest == nil {
				continue
			}
			out.Merge(rest)
		default:
			// It's a wildcard, so all things of that type count
			out.Add(dr.GetNamespace())
		}
	}
	return out, nil
}

func (ts *TypeSystem) getIntermediateRelationTypes(ctx context.Context, defName string, relName string) (*mapz.Set[string], error) {
	def, err := ts.GetDefinition(ctx, defName)
	if err != nil {
		return nil, err
	}
	rel, ok := def.GetRelation(relName)
	if !ok {
		return nil, asTypeError(NewRelationNotFoundErr(defName, relName))
	}
	out := mapz.NewSet[string]()
	for _, dr := range rel.TypeInformation.GetAllowedDirectRelations() {
		out.Add(dr.GetNamespace())
	}
	return out, nil
}

func (ts *TypeSystem) getTypesForRewrite(ctx context.Context, defName string, rel *corev1.UsersetRewrite, seen *mapz.Set[string], nonTerminals *mapz.Set[string]) (*mapz.Set[string], error) {
	out := mapz.NewSet[string]()

	// We're finding the union of all the things touched, regardless.
	toCheck := []*corev1.SetOperation{rel.GetUnion(), rel.GetIntersection(), rel.GetExclusion()}

	for _, op := range toCheck {
		if op == nil {
			continue
		}
		for _, child := range op.GetChild() {
			if computed := child.GetComputedUserset(); computed != nil {
				set, err := ts.getTypesForRelationInternal(ctx, defName, computed.GetRelation(), seen, nonTerminals)
				if err != nil {
					return nil, err
				}
				if set == nil {
					continue
				}
				out.Merge(set)
			}
			if rewrite := child.GetUsersetRewrite(); rewrite != nil {
				sub, err := ts.getTypesForRewrite(ctx, defName, rewrite, seen, nonTerminals)
				if err != nil {
					return nil, err
				}
				out.Merge(sub)
			}
			if userset := child.GetTupleToUserset(); userset != nil {
				set, err := ts.getIntermediateRelationTypes(ctx, defName, userset.GetTupleset().GetRelation())
				if err != nil {
					return nil, err
				}
				for _, s := range set.AsSlice() {
					targets, err := ts.getTypesForRelationInternal(ctx, s, userset.GetComputedUserset().GetRelation(), seen, nonTerminals)
					if err != nil {
						return nil, err
					}
					if targets == nil {
						// Already added
						continue
					}
					out.Merge(targets)
				}
			}
			if functioned := child.GetFunctionedTupleToUserset(); functioned != nil {
				set, err := ts.getIntermediateRelationTypes(ctx, defName, functioned.GetTupleset().GetRelation())
				if err != nil {
					return nil, err
				}
				for _, s := range set.AsSlice() {
					targets, err := ts.getTypesForRelationInternal(ctx, s, functioned.GetComputedUserset().GetRelation(), seen, nonTerminals)
					if err != nil {
						return nil, err
					}
					if targets == nil {
						continue
					}
					out.Merge(targets)
				}
			}
		}
	}
	return out, nil
}
