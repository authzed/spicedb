package types

import (
	"errors"
	"fmt"
	"sync"

	"github.com/authzed/cel-go/cel"
)

// TypeSet defines a set of types that can be used in caveats. It is used to register custom types
// and methods that can be used in caveats. The types are registered by calling the RegisterType
// function. The types are then used to build the CEL environment for the caveat.
type TypeSet struct {
	// definitions holds the set of all types defined and exported by this package, by name.
	definitions map[string]typeDefinition

	// customOptions holds a set of custom options that can be used to create a CEL environment
	// for the caveat.
	customOptions []cel.EnvOption

	// isFrozen indicates whether the TypeSet is frozen. A frozen TypeSet cannot be modified.
	isFrozen bool

	// baseCelEnvOnce guards the construction of baseCelEnv.
	baseCelEnvOnce sync.Once

	// baseCelEnv is the lazily-built, cached CEL environment for this TypeSet.
	// It is only valid once the TypeSet is frozen.
	baseCelEnv    *cel.Env
	baseCelEnvErr error
}

// Freeze marks the TypeSet as frozen. A frozen TypeSet cannot be modified.
func (ts *TypeSet) Freeze() {
	ts.isFrozen = true
}

// EnvOptions returns the set of environment options that can be used to create a CEL environment
// for the caveat. This includes the custom types and methods defined in the TypeSet.
func (ts *TypeSet) EnvOptions() ([]cel.EnvOption, error) {
	if !ts.isFrozen {
		return nil, errors.New("cannot get env options from a non-frozen TypeSet")
	}
	return ts.customOptions, nil
}

// BaseCelEnvironment returns the CEL environment holding the custom types and
// methods defined in the TypeSet, along with the standard options used by all
// caveats.
//
// The returned environment declares *no* caveat parameters: callers must
// derive their own environment from it via Extend, which is significantly
// cheaper than building a full environment from scratch.
//
// Since a frozen TypeSet is immutable, the environment is built once and
// cached for the lifetime of the TypeSet.
func (ts *TypeSet) BaseCelEnvironment() (*cel.Env, error) {
	if !ts.isFrozen {
		return nil, errors.New("cannot build a CEL environment from a non-frozen TypeSet")
	}

	ts.baseCelEnvOnce.Do(func() {
		opts := make([]cel.EnvOption, 0, len(ts.customOptions)+4)
		opts = append(opts, ts.customOptions...)

		// DefaultUTCTimeZone: ensure all timestamps are evaluated at UTC
		opts = append(opts, cel.DefaultUTCTimeZone(true))

		// OptionalTypes: enable optional typing syntax, e.g. `sometype?.foo`
		// See: https://github.com/google/cel-spec/wiki/proposal-246
		opts = append(opts, cel.OptionalTypes(cel.OptionalTypesVersion(0)))

		// EnableMacroCallTracking: enables tracking of call macros so when we call AstToString we get
		// back out the expected expressions.
		// See: https://github.com/authzed/cel-go/issues/474
		opts = append(opts, cel.EnableMacroCallTracking())

		// ParserExpressionSizeLimit: disable the size limit for codepoints in expressions.
		// This has to be disabled due to us padding out the whitespace in expression parsing based on
		// schema size. We instead do our own expression size check in the Compile method.
		// TODO(jschorr): Remove this once the whitespace hack is removed.
		opts = append(opts, cel.ParserExpressionSizeLimit(-1))

		ts.baseCelEnv, ts.baseCelEnvErr = cel.NewEnv(opts...)
	})

	return ts.baseCelEnv, ts.baseCelEnvErr
}

// BuildType builds a variable type from its name and child types.
func (ts *TypeSet) BuildType(name string, childTypes []VariableType) (*VariableType, error) {
	if !ts.isFrozen {
		return nil, errors.New("cannot build types from a non-frozen TypeSet")
	}

	typeDef, ok := ts.definitions[name]
	if !ok {
		return nil, fmt.Errorf("unknown type `%s`", name)
	}

	return typeDef.asVariableType(childTypes)
}

// NewTypeSet creates a new TypeSet. The TypeSet is not frozen and can be modified.
func NewTypeSet() *TypeSet {
	return &TypeSet{
		definitions:   map[string]typeDefinition{},
		customOptions: []cel.EnvOption{},
		isFrozen:      false,
	}
}
