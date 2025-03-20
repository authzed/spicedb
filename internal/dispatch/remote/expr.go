package remote

import (
	"fmt"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"
	"google.golang.org/protobuf/proto"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// DispatchExpr is a CEL expression that can be run to determine the secondary dispatchers, if any,
// to invoke for the incoming request.
type DispatchExpr struct {
	env        *cel.Env
	registry   *types.Registry
	methodName string
	prg        cel.Program
}

var dispatchRequestTypes = []proto.Message{
	&dispatchv1.DispatchCheckRequest{},
	&corev1.RelationReference{},
	&corev1.ObjectAndRelation{},
}

// ParseDispatchExpression parses a dispatch expression via CEL.
func ParseDispatchExpression(methodName string, exprString string) (*DispatchExpr, error) {
	registry, err := types.NewRegistry(dispatchRequestTypes...)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize dispatch expression type registry")
	}

	opts := make([]cel.EnvOption, 0)
	opts = append(opts, cel.OptionalTypes(cel.OptionalTypesVersion(0)))
	opts = append(opts, cel.Variable("request", cel.DynType))

	celEnv, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, err
	}

	ast, issues := celEnv.CompileSource(common.NewStringSource(exprString, methodName))
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}

	if !ast.OutputType().IsEquivalentType(cel.ListType(cel.StringType)) {
		return nil, fmt.Errorf("dispatch expression must result in a list[string] value: found `%s`", ast.OutputType().String())
	}

	runDispatchOps := make([]cel.ProgramOption, 0, 3)
	runDispatchOps = append(runDispatchOps, cel.EvalOptions(cel.OptTrackState))
	runDispatchOps = append(runDispatchOps, cel.EvalOptions(cel.OptPartialEval))
	runDispatchOps = append(runDispatchOps, cel.CostLimit(50))

	prg, err := celEnv.Program(ast, runDispatchOps...)
	if err != nil {
		return nil, err
	}

	return &DispatchExpr{
		env:        celEnv,
		registry:   registry,
		methodName: methodName,
		prg:        prg,
	}, nil
}

// RunDispatchExpr runs a dispatch CEL expression over the given request and returns the secondary dispatchers
// to invoke, if any.
func RunDispatchExpr[R any](de *DispatchExpr, request R) ([]string, error) {
	// Mark any unspecified variables as unknown, to ensure that partial application
	// will result in producing a type of Unknown.
	activation, err := de.env.PartialVars(map[string]any{
		"request": de.registry.NativeToValue(request),
	})
	if err != nil {
		return nil, err
	}

	val, _, err := de.prg.Eval(activation)
	if err != nil {
		return nil, fmt.Errorf("unable to evaluate dispatch expression: %w", err)
	}

	// If the value produced has Unknown type, then it means required context was missing.
	if types.IsUnknown(val) {
		return nil, fmt.Errorf("unable to eval dispatch expression; did you make sure you use `request.`?")
	}

	values := val.Value().([]ref.Val)
	convertedValues := make([]string, 0, len(values))
	for _, value := range values {
		convertedValues = append(convertedValues, value.Value().(string))
	}
	return convertedValues, nil
}
