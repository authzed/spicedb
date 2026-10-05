package dispatch

import (
	"github.com/authzed/spicedb/internal/dispatch/queryopt/dispatchwrap"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
)

// DispatchWrapAliasOptimizationName is the registry name for the
// dispatch-wrap-alias optimization. Callers fetch the registered Optimizer by
// this name when they want to apply the wrap as a standalone step after the
// usual queryopt.OptimizersForRequest set.
const DispatchWrapAliasOptimizationName = dispatchwrap.Name

func init() { queryopt.MustRegisterOptimization(dispatchwrap.New(DispatchIteratorType)) }

// ApplyDispatchWrap runs only the dispatch-wrap-alias optimization on the
// given CanonicalOutline. It exists for callers (dispatch service handlers,
// query plan compilers) that already ran queryopt.OptimizersForRequest and
// want to bolt on the dispatch wrap as a final pass.
func ApplyDispatchWrap(co query.CanonicalOutline, params queryopt.RequestParams) (query.CanonicalOutline, error) {
	opt, err := queryopt.GetOptimization(DispatchWrapAliasOptimizationName)
	if err != nil {
		return query.CanonicalOutline{}, err
	}
	return queryopt.ApplyOptimizations(co, []queryopt.Optimizer{opt}, params)
}
