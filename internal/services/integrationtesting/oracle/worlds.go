package oracle

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/ast"
	celtypes "github.com/authzed/cel-go/common/types"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// maxWorlds bounds the number of worlds, and so the number of clingo runs.
const maxWorlds = 4096

// World is one assignment of values to every caveat parameter that some relationship
// leaves unset. It is the caveat context of a request, in the form a request takes it.
type World map[string]any

// ErrUnsupported is returned for inputs the oracle cannot model yet.
type ErrUnsupported struct{ Reason string }

func (e ErrUnsupported) Error() string { return "unsupported by the oracle: " + e.Reason }

// paramType is the type of a caveat parameter, as written in the schema.
type paramType struct {
	name  string
	child *paramType // for list and map
}

func (pt paramType) String() string {
	if pt.child != nil {
		return pt.name + "<" + pt.child.String() + ">"
	}
	return pt.name
}

func decodeParamType(ref *core.CaveatTypeReference) (paramType, error) {
	pt := paramType{name: ref.TypeName}
	switch ref.TypeName {
	case "list", "map":
		if len(ref.ChildTypes) != 1 {
			return paramType{}, fmt.Errorf("%s requires one child type", ref.TypeName)
		}
		child, err := decodeParamType(ref.ChildTypes[0])
		if err != nil {
			return paramType{}, err
		}
		pt.child = &child
	case "ipaddress":
		// A SpiceDB extension to CEL rather than part of CEL, so not available here.
		return paramType{}, ErrUnsupported{"parameters of type ipaddress"}
	}
	return pt, nil
}

// celType returns the CEL type the parameter is declared as. Note that uint parameters are
// declared as CEL ints, so that they compare with integer literals.
func (pt paramType) celType() (*cel.Type, error) {
	switch pt.name {
	case "any":
		return cel.DynType, nil
	case "bool":
		return cel.BoolType, nil
	case "string":
		return cel.StringType, nil
	case "int", "uint":
		return cel.IntType, nil
	case "double":
		return cel.DoubleType, nil
	case "bytes":
		return cel.BytesType, nil
	case "duration":
		return cel.DurationType, nil
	case "timestamp":
		return cel.TimestampType, nil
	case "list":
		child, err := pt.child.celType()
		if err != nil {
			return nil, err
		}
		return cel.ListType(child), nil
	case "map":
		child, err := pt.child.celType()
		if err != nil {
			return nil, err
		}
		return cel.MapType(cel.StringType, child), nil
	default:
		return nil, fmt.Errorf("unknown parameter type %q", pt.name)
	}
}

// convert turns a context value, as it appears in JSON, into the value CEL expects.
func (pt paramType) convert(value any) (any, error) {
	fail := func() (any, error) {
		return nil, fmt.Errorf("a %s value is required, found %T `%v`", pt, value, value)
	}

	switch pt.name {
	case "any":
		return value, nil

	case "bool":
		if v, ok := value.(bool); ok {
			return v, nil
		}
		return fail()

	case "string":
		if v, ok := value.(string); ok {
			return v, nil
		}
		return fail()

	case "int", "uint":
		var n int64
		switch v := value.(type) {
		case float64:
			if v != math.Trunc(v) {
				return fail()
			}
			n = int64(v)
		case int64:
			n = v
		case uint64:
			n = int64(v)
		default:
			return fail()
		}
		if pt.name == "uint" && n < 0 {
			return fail()
		}
		return n, nil

	case "double":
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		}
		return fail()

	case "bytes":
		s, ok := value.(string)
		if !ok {
			return fail()
		}
		return base64.StdEncoding.DecodeString(s)

	case "duration":
		s, ok := value.(string)
		if !ok {
			return fail()
		}
		return time.ParseDuration(s)

	case "timestamp":
		s, ok := value.(string)
		if !ok {
			return fail()
		}
		return time.Parse(time.RFC3339, s)

	case "list":
		items, ok := value.([]any)
		if !ok {
			return fail()
		}
		out := make([]any, 0, len(items))
		for _, item := range items {
			converted, err := pt.child.convert(item)
			if err != nil {
				return nil, err
			}
			out = append(out, converted)
		}
		return out, nil

	case "map":
		entries, ok := value.(map[string]any)
		if !ok {
			return fail()
		}
		out := make(map[string]any, len(entries))
		for k, entry := range entries {
			converted, err := pt.child.convert(entry)
			if err != nil {
				return nil, err
			}
			out[k] = converted
		}
		return out, nil

	default:
		return fail()
	}
}

type caveat struct {
	name    string
	params  map[string]paramType
	ast     *cel.Ast
	program cel.Program
}

// compileCaveat compiles a caveat with cel-go alone. The stored form is a checked CEL
// expression; it is turned back into source and compiled afresh, so that nothing of
// SpiceDB's own compilation is reused.
func compileCaveat(def *core.CaveatDefinition) (caveat, error) {
	cav := caveat{name: def.Name, params: map[string]paramType{}}

	opts := []cel.EnvOption{
		// The language options SpiceDB defines caveats with: timestamps are in UTC, and
		// optional syntax (e.g. `a.?b`) is allowed.
		cel.DefaultUTCTimeZone(true),
		cel.OptionalTypes(),
	}
	for name, ref := range def.ParameterTypes {
		pt, err := decodeParamType(ref)
		if err != nil {
			return caveat{}, fmt.Errorf("caveat %q, parameter %q: %w", def.Name, name, err)
		}
		celType, err := pt.celType()
		if err != nil {
			return caveat{}, err
		}
		cav.params[name] = pt
		opts = append(opts, cel.Variable(name, celType))
	}
	env, err := cel.NewEnv(opts...)
	if err != nil {
		return caveat{}, err
	}

	decoded := &impl.DecodedCaveat{}
	if err := decoded.UnmarshalVT(def.SerializedExpression); err != nil {
		return caveat{}, err
	}
	source, err := cel.AstToString(cel.CheckedExprToAst(decoded.GetCel()))
	if err != nil {
		return caveat{}, err
	}

	compiled, iss := env.Compile(source)
	if iss.Err() != nil {
		return caveat{}, fmt.Errorf("caveat %q: %w", def.Name, iss.Err())
	}
	if compiled.OutputType() != cel.BoolType {
		return caveat{}, fmt.Errorf("caveat %q does not produce a bool", def.Name)
	}
	program, err := env.Program(compiled)
	if err != nil {
		return caveat{}, err
	}
	cav.ast, cav.program = compiled, program
	return cav, nil
}

// evaluate runs the caveat on a complete context, given as JSON values.
func (cav caveat) evaluate(context map[string]any) (bool, error) {
	converted := make(map[string]any, len(cav.params))
	for name, pt := range cav.params {
		raw, ok := context[name]
		if !ok {
			return false, fmt.Errorf("caveat %q: missing parameter %q", cav.name, name)
		}
		v, err := pt.convert(raw)
		if err != nil {
			return false, fmt.Errorf("caveat %q, parameter %q: %w", cav.name, name, err)
		}
		converted[name] = v
	}

	result, _, err := cav.program.Eval(converted)
	if err != nil {
		return false, fmt.Errorf("caveat %q: %w", cav.name, err)
	}
	b, ok := result.(celtypes.Bool)
	if !ok {
		return false, fmt.Errorf("caveat %q produced %v", cav.name, result)
	}
	return bool(b), nil
}

// caveatEnv holds the compiled caveats of a validation file, and the worlds that must be
// considered to evaluate them exactly.
type caveatEnv struct {
	caveats map[string]caveat
	worlds  []World
}

func newCaveatEnv(defs []*core.CaveatDefinition, rels []tuple.Relationship) (*caveatEnv, error) {
	env := &caveatEnv{caveats: map[string]caveat{}}
	for _, def := range defs {
		cav, err := compileCaveat(def)
		var unsupported ErrUnsupported
		if errors.As(err, &unsupported) && !isUsed(def.Name, rels) {
			continue
		}
		if err != nil {
			return nil, err
		}
		env.caveats[def.Name] = cav
	}

	// A parameter is open if some relationship uses a caveat without setting it: the request
	// context decides its value, so every value it can take must be considered.
	openTypes := map[string]paramType{}
	for _, rel := range rels {
		if rel.OptionalCaveat == nil {
			continue
		}
		cav, ok := env.caveats[rel.OptionalCaveat.CaveatName]
		if !ok {
			return nil, fmt.Errorf("unknown caveat %q", rel.OptionalCaveat.CaveatName)
		}
		written := rel.OptionalCaveat.GetContext().AsMap()
		for name, pt := range cav.params {
			if _, ok := written[name]; ok {
				continue
			}
			if existing, ok := openTypes[name]; ok && existing.String() != pt.String() {
				return nil, ErrUnsupported{fmt.Sprintf("parameter %q has types %s and %s", name, existing, pt)}
			}
			openTypes[name] = pt
		}
	}

	literals, err := env.literals(rels)
	if err != nil {
		return nil, err
	}

	names := slices.Sorted(maps.Keys(openTypes))
	domains := make([][]any, 0, len(names))
	count := 1
	for _, name := range names {
		domain, err := domainFor(openTypes[name], literals)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", name, err)
		}
		domains = append(domains, domain)
		count *= len(domain)
		if count > maxWorlds {
			return nil, ErrUnsupported{fmt.Sprintf("more than %d worlds", maxWorlds)}
		}
	}

	// The worlds are the cartesian product of the domains.
	env.worlds = []World{{}}
	for i, name := range names {
		next := make([]World, 0, len(env.worlds)*len(domains[i]))
		for _, w := range env.worlds {
			for _, v := range domains[i] {
				nw := maps.Clone(w)
				nw[name] = v
				next = append(next, nw)
			}
		}
		env.worlds = next
	}
	return env, nil
}

func isUsed(caveatName string, rels []tuple.Relationship) bool {
	return slices.ContainsFunc(rels, func(rel tuple.Relationship) bool {
		return rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName == caveatName
	})
}

// literals returns every value that could matter to a comparison, keyed by the name of its
// type: the constants in the caveat expressions, and the values written on relationships.
func (env *caveatEnv) literals(rels []tuple.Relationship) (map[string][]any, error) {
	found := map[string][]any{}
	add := func(kind string, v any) { found[kind] = append(found[kind], v) }
	addValue := func(v any) {
		switch v := v.(type) {
		case int64:
			add("int", v)
		case float64:
			add("double", v)
		case string:
			add("string", v)
			if _, err := time.Parse(time.RFC3339, v); err == nil {
				add("timestamp", v)
			}
		case time.Time:
			add("timestamp", v.UTC().Format(time.RFC3339))
		}
	}

	for _, cav := range env.caveats {
		ast.PreOrderVisit(cav.ast.NativeRep().Expr(), ast.NewExprVisitor(func(e ast.Expr) {
			if e.Kind() != ast.LiteralKind {
				return
			}
			switch v := e.AsLiteral().Value().(type) {
			case uint64:
				addValue(int64(v))
			default:
				addValue(v)
			}
		}))
	}

	for _, rel := range rels {
		if rel.OptionalCaveat == nil {
			continue
		}
		cav := env.caveats[rel.OptionalCaveat.CaveatName]
		for name, raw := range rel.OptionalCaveat.GetContext().AsMap() {
			pt, ok := cav.params[name]
			if !ok {
				continue
			}
			converted, err := pt.convert(raw)
			if err != nil {
				return nil, err
			}
			addValue(converted)
		}
	}
	return found, nil
}

// domainFor returns the values a parameter of the given type takes across worlds. It is
// every literal of that type plus its neighbours, which reaches every outcome of
// comparisons between parameters and literals.
func domainFor(pt paramType, literals map[string][]any) ([]any, error) {
	switch pt.name {
	case "bool":
		return []any{false, true}, nil

	case "int", "uint":
		values := map[int64]struct{}{0: {}}
		for _, l := range literals["int"] {
			v := l.(int64)
			values[v-1], values[v], values[v+1] = struct{}{}, struct{}{}, struct{}{}
		}
		sorted := slices.Sorted(maps.Keys(values))
		out := make([]any, 0, len(sorted))
		for _, v := range sorted {
			switch {
			case pt.name == "int":
				out = append(out, v)
			case v >= 0:
				out = append(out, uint64(v))
			}
		}
		return out, nil

	case "double":
		values := map[float64]struct{}{0: {}}
		for _, l := range literals["double"] {
			v := l.(float64)
			values[v-0.5], values[v], values[v+0.5] = struct{}{}, struct{}{}, struct{}{}
		}
		return toAny(slices.Sorted(maps.Keys(values))), nil

	case "string":
		values := map[string]struct{}{"": {}, "\x00not-a-literal": {}}
		for _, l := range literals["string"] {
			values[l.(string)] = struct{}{}
		}
		return toAny(slices.Sorted(maps.Keys(values))), nil

	case "timestamp":
		values := map[string]struct{}{}
		for _, l := range literals["timestamp"] {
			t, _ := time.Parse(time.RFC3339, l.(string))
			for _, d := range []time.Duration{-time.Second, 0, time.Second} {
				values[t.Add(d).UTC().Format(time.RFC3339)] = struct{}{}
			}
		}
		if len(values) == 0 {
			values[time.Unix(0, 0).UTC().Format(time.RFC3339)] = struct{}{}
		}
		return toAny(slices.Sorted(maps.Keys(values))), nil

	default:
		return nil, ErrUnsupported{"open parameters of type " + pt.String()}
	}
}

func toAny[T any](values []T) []any {
	out := make([]any, 0, len(values))
	for _, v := range values {
		out = append(out, v)
	}
	return out
}

// holds reports whether the caveat on the relationship is satisfied in the world. The
// context written on the relationship takes precedence over the world.
func (env *caveatEnv) holds(rel tuple.Relationship, world World) (bool, error) {
	if rel.OptionalCaveat == nil {
		return true, nil
	}
	merged := maps.Clone(world)
	maps.Copy(merged, rel.OptionalCaveat.GetContext().AsMap())
	return env.caveats[rel.OptionalCaveat.CaveatName].evaluate(merged)
}
