package decorators

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ValueKind is the syntactic kind of a decorator parameter value, as parsed.
//
// NOTE: there is no distinct number kind. The lexer never emits TokenTypeNumber
// (isAlphaNumeric includes digits), so `16` arrives as an identifier. The declared
// parameter type is what decides how a value is coerced.
type ValueKind string

const (
	// ValueKindString is a quote-delimited string.
	ValueKindString ValueKind = "string"

	// ValueKindIdentifier is a bare token: a number, `true`/`false`, or an enum value.
	ValueKindIdentifier ValueKind = "identifier"
)

// Value is a decorator parameter value as parsed: for a string value, the delimiting
// quote characters have already been stripped by the parser's tryConsumeStringLiteral,
// so Raw holds the string's contents, not its literal source text.
type Value struct {
	Kind ValueKind
	Raw  string
}

// AppliedParameter is one `name: value` argument as written in a schema.
type AppliedParameter struct {
	Name  string
	Value Value
}

// Applied is a decorator as written in a schema, before validation.
type Applied struct {
	Name       string
	Parameters []AppliedParameter
}

// Validate checks an applied decorator against the registry and, on success, returns
// its compiled proto form.
//
// flagEnabled reports whether the schema declared `use <flag>`; flagAllowed reports
// whether the deployment permits that flag.
func (r Registry) Validate(
	applied Applied,
	site Site,
	flagEnabled func(string) bool,
	flagAllowed func(string) bool,
) (*core.Decorator, error) {
	spec, ok := r[applied.Name]
	if !ok {
		if len(r) == 0 {
			return nil, fmt.Errorf("unknown decorator `@%s`", applied.Name)
		}
		return nil, fmt.Errorf("unknown decorator `@%s`. Options are: %s",
			applied.Name, strings.Join(r.Names(), ", "))
	}

	if !flagEnabled(spec.RequiredFlag) {
		return nil, fmt.Errorf("decorator `@%s` requires `use %s`", applied.Name, spec.RequiredFlag)
	}

	if !flagAllowed(spec.RequiredFlag) {
		return nil, fmt.Errorf("the `%s` flag is not allowed", spec.RequiredFlag)
	}

	if !spec.AllowsSite(site) {
		return nil, fmt.Errorf("decorator `@%s` is not permitted on a %s", applied.Name, site)
	}

	seen := make(map[string]struct{}, len(applied.Parameters))
	byName := make(map[string]Value, len(applied.Parameters))

	for _, param := range applied.Parameters {
		if _, found := seen[param.Name]; found {
			return nil, fmt.Errorf("parameter `%s` specified more than once for decorator `@%s`",
				param.Name, applied.Name)
		}
		seen[param.Name] = struct{}{}

		if _, found := spec.Parameter(param.Name); !found {
			return nil, fmt.Errorf("unknown parameter `%s` for decorator `@%s`. Options are: %s",
				param.Name, applied.Name, strings.Join(spec.ParameterNames(), ", "))
		}

		byName[param.Name] = param.Value
	}

	// Emit parameters in the spec's canonical order so that generated schemas are stable.
	compiled := make([]*core.DecoratorParameter, 0, len(applied.Parameters))
	for _, declared := range spec.Parameters {
		value, found := byName[declared.Name]
		if !found {
			if declared.Required {
				return nil, fmt.Errorf("missing required parameter `%s` for decorator `@%s`",
					declared.Name, applied.Name)
			}
			continue
		}

		converted, err := coerce(applied.Name, declared, value)
		if err != nil {
			return nil, err
		}

		compiled = append(compiled, converted)
	}

	return &core.Decorator{
		Name:         applied.Name,
		RequiredFlag: spec.RequiredFlag,
		Parameters:   compiled,
	}, nil
}

func coerce(decoratorName string, declared Parameter, value Value) (*core.DecoratorParameter, error) {
	switch declared.Type {
	case ParamTypeInt:
		if value.Kind != ValueKindIdentifier {
			return nil, intError(decoratorName, declared)
		}
		parsed, err := strconv.ParseInt(value.Raw, 10, 64)
		if err != nil {
			return nil, intError(decoratorName, declared)
		}
		return &core.DecoratorParameter{
			Name:  declared.Name,
			Value: &core.DecoratorParameter_IntValue{IntValue: parsed},
		}, nil

	case ParamTypeString:
		if value.Kind != ValueKindString {
			return nil, fmt.Errorf("parameter `%s` of decorator `@%s` expects a quoted string",
				declared.Name, decoratorName)
		}

		// The schema DSL has no backslash-escape syntax anywhere: the lexer's string
		// scanner reads raw characters up to the closing quote, and the parser just
		// trims the quote characters off the token. That means a string value can only
		// be regenerated as valid schema source if a delimiter exists that it does not
		// contain (the generator picks `"` unless the value contains one, then falls
		// back to `'`), and it cannot span multiple lines. Reject values that violate
		// this here, at the source position, rather than let them reach the proto and
		// silently corrupt the generator's output (and, transitively, the schema hash).
		// Do NOT "fix" this by adding escape support here without also teaching the
		// lexer (lexStringLiteral) and parser (tryConsumeStringLiteral) to understand it.
		if strings.ContainsRune(value.Raw, '"') && strings.ContainsRune(value.Raw, '\'') {
			return nil, fmt.Errorf("parameter `%s` of decorator `@%s` contains characters that cannot be represented in a schema string: a value may not contain both quote styles",
				declared.Name, decoratorName)
		}
		if strings.ContainsAny(value.Raw, "\n\r") {
			return nil, fmt.Errorf("parameter `%s` of decorator `@%s` may not contain a newline",
				declared.Name, decoratorName)
		}

		return &core.DecoratorParameter{
			Name:  declared.Name,
			Value: &core.DecoratorParameter_StringValue{StringValue: value.Raw},
		}, nil

	case ParamTypeBool:
		if value.Kind != ValueKindIdentifier || (value.Raw != "true" && value.Raw != "false") {
			return nil, fmt.Errorf("parameter `%s` of decorator `@%s` expects true or false",
				declared.Name, decoratorName)
		}
		return &core.DecoratorParameter{
			Name:  declared.Name,
			Value: &core.DecoratorParameter_BoolValue{BoolValue: value.Raw == "true"},
		}, nil

	case ParamTypeEnum:
		if value.Kind != ValueKindIdentifier || !slices.Contains(declared.EnumValues, value.Raw) {
			return nil, fmt.Errorf("invalid value `%s` for parameter `%s` of decorator `@%s`; expected one of: %s",
				value.Raw, declared.Name, decoratorName, strings.Join(declared.EnumValues, ", "))
		}
		return &core.DecoratorParameter{
			Name:  declared.Name,
			Value: &core.DecoratorParameter_EnumValue{EnumValue: value.Raw},
		}, nil

	default:
		return nil, fmt.Errorf("decorator `@%s` declares parameter `%s` with unknown type `%s`",
			decoratorName, declared.Name, declared.Type)
	}
}

func intError(decoratorName string, declared Parameter) error {
	return fmt.Errorf("parameter `%s` of decorator `@%s` expects an integer",
		declared.Name, decoratorName)
}
