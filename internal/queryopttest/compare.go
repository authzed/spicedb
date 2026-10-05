package queryopttest

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/tuple"
)

// decision distinguishes denial, unconditional access and residual partial access.
type decision struct {
	Allowed bool
	Missing []string
}

func evaluate(ctx context.Context, reader caveats.CaveatDefinitionLookup, expr *core.CaveatExpression, values map[string]any) (decision, error) {
	if expr == nil {
		return decision{Allowed: true}, nil
	}
	result, err := caveats.RunSingleCaveatExpression(ctx, types.Default.TypeSet, expr, values, reader, caveats.RunCaveatExpressionNoDebugging)
	if err != nil {
		return decision{}, err
	}
	if result.IsPartial() {
		missing, err := result.MissingVarNames()
		if err != nil {
			return decision{}, err
		}
		slices.Sort(missing)
		return decision{Allowed: true, Missing: missing}, nil
	}
	return decision{Allowed: result.Value()}, nil
}

func orExpr(a, b *core.CaveatExpression) *core.CaveatExpression {
	if a == nil || b == nil {
		return nil
	}
	return caveats.Or(a, b)
}

func compareResults(ctx context.Context, reader caveats.CaveatDefinitionLookup, op query.Operation, left, right []*query.Path, values map[string]any) error {
	// All explicitly mentioned IDs, including excluded ones, plus '*' as the
	// generic unmentioned subject class. This checks wildcard meaning without
	// depending on how an iterator splits wildcard and concrete grants.
	ids := map[string]bool{}
	var collect func(*query.Path)
	collect = func(p *query.Path) {
		if p.Subject.ObjectID != tuple.PublicWildcard {
			ids[p.Subject.ObjectID] = true
		}
		for _, e := range p.ExcludedSubjects {
			collect(e)
		}
	}
	for _, p := range left {
		collect(p)
	}
	for _, p := range right {
		collect(p)
	}
	domain := slices.Sorted(maps.Keys(ids))
	domain = append(domain, tuple.PublicWildcard)
	normalize := func(paths []*query.Path) (map[string]decision, error) {
		exprs := map[string]*core.CaveatExpression{}
		for _, p := range paths {
			subjects := []string{p.Subject.ObjectID}
			if p.Subject.ObjectID == tuple.PublicWildcard && op == query.OperationIterSubjects {
				subjects = domain
			}
			for _, id := range subjects {
				expr, present := p.Caveat, true
				if p.Subject.ObjectID == tuple.PublicWildcard && id != tuple.PublicWildcard {
					for _, excluded := range p.ExcludedSubjects {
						if excluded.Subject.ObjectType != p.Subject.ObjectType || excluded.Subject.Relation != p.Subject.Relation || excluded.Subject.ObjectID != id {
							continue
						}
						if excluded.Caveat == nil {
							present = false
							break
						}
						expr = caveats.And(expr, caveats.Invert(excluded.Caveat))
					}
				}
				if !present {
					continue
				}
				subject := p.Subject
				subject.ObjectID = id
				key := fmt.Sprintf("%s#%s@%s", p.Resource.Key(), p.Relation, tuple.StringONR(subject))
				if op == query.OperationCheck {
					key = "check"
				}
				if previous, ok := exprs[key]; ok {
					expr = orExpr(previous, expr)
				}
				exprs[key] = expr
			}
		}
		result := map[string]decision{}
		for _, key := range slices.Sorted(maps.Keys(exprs)) {
			d, err := evaluate(ctx, reader, exprs[key], values)
			if err != nil {
				return nil, err
			}
			if d.Allowed {
				result[key] = d
			}
		}
		return result, nil
	}
	a, err := normalize(left)
	if err != nil {
		return err
	}
	b, err := normalize(right)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("results differ: before=%v after=%v", a, b)
	}
	return nil
}
