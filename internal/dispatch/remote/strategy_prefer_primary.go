package remote

import (
	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types/ref"
)

// prefer_primary() dispatches to the primary only: it is equivalent to hedged([]), and reads
// better where an expression chooses between strategies, such as
//
//	request.resource_relation.namespace == 'document'
//	  ? prefer_secondary(['tiger'], duration('50ms'))
//	  : prefer_primary()
//
// With no secondaries there is no race: the request takes the primary-only path.

const preferPrimaryFunction = "prefer_primary"

var preferPrimaryDescriptor = strategyDescriptor{
	function:    preferPrimaryFunction,
	primaryOnly: true,
	overloads: []cel.FunctionOpt{
		cel.Overload("prefer_primary_none",
			[]*cel.Type{}, dispatchPlanCelType,
			cel.FunctionBinding(func(...ref.Val) ref.Val {
				return dispatchPlanVal{dispatchPlan{secondaries: []string{}, strategy: hedgedStrategy{}}}
			})),
	},
}
