package remote

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"
	"github.com/authzed/cel-go/common/types/traits"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// DispatchExpr is a CEL expression that can be run to determine the secondary dispatchers, if any,
// to invoke for the incoming request, and the strategy used to race them against the primary.
//
// The expression returns either a list of secondary names, which are raced with the default
// strategy (hedged), or the result of a strategy function, such as hedged([...]) or
// prefer_secondary([...], duration('50ms')).
type DispatchExpr struct {
	env        *cel.Env
	registry   *types.Registry
	methodName string
	prg        cel.Program
}

var dispatchRequestTypes = []any{
	&dispatchv1.DispatchCheckRequest{},
	&corev1.RelationReference{},
	&corev1.ObjectAndRelation{},
}

// strategyDescriptor declares the CEL function of a secondary strategy. Each overload's binding
// must return a dispatchPlanVal.
type strategyDescriptor struct {
	function  string
	overloads []cel.FunctionOpt
}

// registeredStrategies lists the strategies available to dispatch expressions. Adding a
// strategy means adding its descriptor here.
var registeredStrategies = []strategyDescriptor{
	hedgedDescriptor,
	preferSecondaryDescriptor,
}

// dispatchPlanCelType is the CEL type returned by strategy functions.
var dispatchPlanCelType = cel.OpaqueType("DispatchPlan")

// dispatchPlan is the result of evaluating a dispatch expression: the secondaries to use, and
// the strategy used to race them against the primary.
type dispatchPlan struct {
	secondaries []string
	strategy    secondaryStrategy
}

// dispatchPlanVal adapts dispatchPlan to a CEL value.
type dispatchPlanVal struct {
	plan dispatchPlan
}

func (v dispatchPlanVal) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("type conversion error from 'DispatchPlan' to '%v'", typeDesc)
}

func (v dispatchPlanVal) ConvertToType(typeVal ref.Type) ref.Val {
	if typeVal == types.TypeType {
		return dispatchPlanCelType
	}
	return types.NewErr("type conversion error from 'DispatchPlan' to '%s'", typeVal)
}

func (v dispatchPlanVal) Equal(other ref.Val) ref.Val {
	return types.MaybeNoSuchOverloadErr(other)
}

func (v dispatchPlanVal) Type() ref.Type {
	return dispatchPlanCelType
}

func (v dispatchPlanVal) Value() any {
	return v.plan
}

// toStringSlice converts a CEL list of strings, without reflection: this runs on every
// dispatch with secondaries.
func toStringSlice(v ref.Val) ([]string, error) {
	if values, ok := v.Value().([]ref.Val); ok {
		names := make([]string, 0, len(values))
		for _, value := range values {
			name, ok := value.(types.String)
			if !ok {
				return nil, fmt.Errorf("expected a list of strings, found an element of type `%s`", value.Type())
			}
			names = append(names, string(name))
		}
		return names, nil
	}

	lister, ok := v.(traits.Lister)
	if !ok {
		return nil, fmt.Errorf("expected a list of strings, found `%s`", v.Type())
	}
	size, ok := lister.Size().(types.Int)
	if !ok {
		return nil, fmt.Errorf("expected a list of strings, found `%s`", v.Type())
	}
	names := make([]string, 0, int(size))
	for i := types.Int(0); i < size; i++ {
		name, ok := lister.Get(i).(types.String)
		if !ok {
			return nil, fmt.Errorf("expected a list of strings, found an element of type `%s`", lister.Get(i).Type())
		}
		names = append(names, string(name))
	}
	return names, nil
}

// ParseDispatchExpression parses a dispatch expression via CEL.
func ParseDispatchExpression(methodName string, exprString string) (*DispatchExpr, error) {
	registry, err := types.NewRegistry(dispatchRequestTypes...)
	if err != nil {
		return nil, errors.New("unable to initialize dispatch expression type registry")
	}

	opts := make([]cel.EnvOption, 0, 2+len(registeredStrategies))
	opts = append(opts,
		cel.OptionalTypes(cel.OptionalTypesVersion(0)),
		cel.Variable("request", cel.DynType),
	)
	for _, strategy := range registeredStrategies {
		opts = append(opts, cel.Function(strategy.function, strategy.overloads...))
	}

	celEnv, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, err
	}

	ast, issues := celEnv.CompileSource(common.NewStringSource(exprString, methodName))
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}

	outputType := ast.OutputType()
	if !outputType.IsEquivalentType(cel.ListType(cel.StringType)) && !outputType.IsEquivalentType(dispatchPlanCelType) {
		return nil, fmt.Errorf("dispatch expression must result in a list[string] value or a strategy such as hedged([...]): found `%s`", outputType.String())
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

// runDispatchExpr runs a dispatch CEL expression over the given request and returns the
// secondary dispatchers to invoke, if any, and the strategy to race them with.
func runDispatchExpr[R any](de *DispatchExpr, request R) (dispatchPlan, error) {
	// Mark any unspecified variables as unknown, to ensure that partial application
	// will result in producing a type of Unknown.
	activation, err := de.env.PartialVars(map[string]any{
		"request": de.registry.NativeToValue(request),
	})
	if err != nil {
		return dispatchPlan{}, err
	}

	val, _, err := de.prg.Eval(activation)
	if err != nil {
		return dispatchPlan{}, fmt.Errorf("unable to evaluate dispatch expression: %w", err)
	}

	// If the value produced has Unknown type, then it means required context was missing.
	if types.IsUnknown(val) {
		return dispatchPlan{}, errors.New("unable to eval dispatch expression; did you make sure you use `request.`?")
	}

	switch v := val.(type) {
	case dispatchPlanVal:
		return v.plan, nil

	case traits.Lister:
		names, err := toStringSlice(v)
		if err != nil {
			return dispatchPlan{}, fmt.Errorf("unable to convert dispatch expression result: %w", err)
		}
		return dispatchPlan{secondaries: names, strategy: hedgedStrategy{}}, nil

	default:
		return dispatchPlan{}, fmt.Errorf("unexpected dispatch expression result type `%s`", val.Type())
	}
}
