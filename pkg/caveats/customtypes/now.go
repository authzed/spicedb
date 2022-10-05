package customtypes

import (
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

func init() {
	registerCustomType("now",
		cel.Function("now",
			cel.Overload("now_timestamp", []*cel.Type{}, cel.TimestampType,
				cel.FunctionBinding(func(arg ...ref.Val) ref.Val {
					return types.Timestamp{Time: time.Now()}
				}))),
	)
}
