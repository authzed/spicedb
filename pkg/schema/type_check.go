package schema

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const ellipsesRelation = "..."

func (ts *TypeSystem) GetTypesForRelation(ctx context.Context, defName string, relationName string) (*mapz.Set[string], error) {
	seen := mapz.NewSet[string]()
	return ts.getTypesForRelationInternal(ctx, defName, relationName, seen)
}

func (ts *TypeSystem) getTypesForRelationInternal(ctx context.Context, defName string, relationName string, seen *mapz.Set[string]) (*mapz.Set[string], error) {
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
		return nil, asTypeError(NewRelationNotFoundErr(defName, relationName))
	}
	if rel.TypeInformation != nil {
		return ts.getTypesForInfo(ctx, defName, rel.TypeInformation, seen)
	} else if rel.UsersetRewrite != nil {
		return ts.getTypesForRewrite(ctx, defName, rel.UsersetRewrite, seen)
	}
	return nil, asTypeError(NewMissingAllowedRelationsErr(defName, relationName))
}

func (ts *TypeSystem) getTypesForInfo(ctx context.Context, defName string, rel *corev1.TypeInformation, seen *mapz.Set[string]) (*mapz.Set[string], error) {
	out := mapz.NewSet[string]()
	for _, dr := range rel.GetAllowedDirectRelations() {
		if dr.GetRelation() == ellipsesRelation {
			out.Add(dr.GetNamespace())
		} else if dr.GetRelation() != "" {
			rest, err := ts.getTypesForRelationInternal(ctx, dr.GetNamespace(), dr.GetRelation(), seen)
			if err != nil {
				return nil, err
			}
			out.Merge(rest)
		} else {
			// It's a wildcard, so all things of that type count
			out.Add(dr.GetNamespace())
		}

	}
	return out, nil
}

func (ts *TypeSystem) getTypesForRewrite(ctx context.Context, defName string, rel *corev1.UsersetRewrite, seen *mapz.Set[string]) (*mapz.Set[string], error) {
	out := mapz.NewSet[string]()

	// We're finding the union of all the things touched, regardless.
	toCheck := []*corev1.SetOperation{rel.GetUnion(), rel.GetIntersection(), rel.GetExclusion()}

	for _, op := range toCheck {
		if op == nil {
			continue
		}
		for _, child := range op.GetChild() {
			if computed := child.GetComputedUserset(); computed != nil {
				set, err := ts.getTypesForRelationInternal(ctx, defName, computed.GetRelation(), seen)
				if err != nil {
					return nil, err
				}
				out.Merge(set)
			}
			if rewrite := child.GetUsersetRewrite(); rewrite != nil {
				sub, err := ts.getTypesForRewrite(ctx, defName, rewrite, seen)
				if err != nil {
					return nil, err
				}
				out.Merge(sub)
			}
			if userset := child.GetTupleToUserset(); userset != nil {
				set, err := ts.getTypesForRelationInternal(ctx, defName, userset.GetTupleset().GetRelation(), seen)
				if err != nil {
					return nil, err
				}
				for _, s := range set.AsSlice() {
					targets, err := ts.getTypesForRelationInternal(ctx, s, userset.GetComputedUserset().GetRelation(), seen)
					if err != nil {
						return nil, err
					}
					out.Merge(targets)
				}
			}
			if functioned := child.GetFunctionedTupleToUserset(); functioned != nil {
				set, err := ts.getTypesForRelationInternal(ctx, defName, functioned.GetTupleset().GetRelation(), seen)
				if err != nil {
					return nil, err
				}
				for _, s := range set.AsSlice() {
					targets, err := ts.getTypesForRelationInternal(ctx, s, functioned.GetComputedUserset().GetRelation(), seen)
					if err != nil {
						return nil, err
					}
					out.Merge(targets)
				}
			}
		}
	}
	return out, nil
}
