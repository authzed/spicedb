// Copyright 2023-2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protovalidate

import (
	"fmt"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	pvcel "buf.build/go/protovalidate/cel"
	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// cache is a build-through cache to computed standard rules.
type cache struct {
	cache map[protoreflect.FieldDescriptor]astSet
}

// newCache constructs a new build-through cache for the standard rules.
func newCache() cache {
	return cache{
		cache: map[protoreflect.FieldDescriptor]astSet{},
	}
}

// Build creates the standard rules for the given field. If forItems is
// true, the rules for repeated list items are built instead of the
// rules on the list itself.
func (c *cache) Build(
	env *cel.Env,
	fieldDesc protoreflect.FieldDescriptor,
	fieldRules *validate.FieldRules,
	extensionTypeResolver protoregistry.ExtensionTypeResolver,
	allowUnknownFields bool,
	forItems bool,
) (set programSet, err error) {
	set.env = env
	rules, setOneof, done, err := c.resolveRules(
		fieldDesc,
		fieldRules,
		forItems,
	)
	if done {
		return set, err
	}

	if err = reparseUnrecognized(extensionTypeResolver, rules); err != nil {
		return set, &CompilationError{cause: fmt.Errorf("error reparsing message: %w", err)}
	}
	if !allowUnknownFields && len(rules.GetUnknown()) > 0 {
		return set, &CompilationError{cause: fmt.Errorf("unknown rules in %s; see protovalidate.WithExtensionTypeResolver", rules.Descriptor().FullName())}
	}

	set.env, err = c.prepareEnvironment(env, fieldDesc, rules, forItems)
	if err != nil {
		return set, err
	}

	var asts astSet
	rules.Range(func(desc protoreflect.FieldDescriptor, rule protoreflect.Value) bool {
		// Try compiling without the rule variable first. Extending a cel
		// environment is expensive.
		precomputedASTs, compileErr := c.loadOrCompileStandardRule(set.env, setOneof, desc)
		if compileErr != nil {
			fieldEnv, compileErr := extendEnv(set.env,
				cel.Variable("rule", pvcel.ProtoFieldToType(desc, true, false)),
			)
			if compileErr != nil {
				err = compileErr
				return false
			}
			precomputedASTs, compileErr = c.loadOrCompileStandardRule(fieldEnv, setOneof, desc)
			if compileErr != nil {
				err = compileErr
				return false
			}
		}
		precomputedASTs, compileErr = precomputedASTs.WithRuleValues(rules, rule, desc)
		if compileErr != nil {
			err = compileErr
			return false
		}
		asts = asts.Merge(precomputedASTs)
		return true
	})
	if err != nil {
		return set, err
	}

	return asts.ReduceResiduals(rules)
}

// resolveRules extracts the standard rules for the specified field. An
// error is returned if the wrong rules are applied to a field (typically
// if there is a type-mismatch). The done result is true if an error is returned
// or if there are now standard rules to apply to this field.
func (c *cache) resolveRules(
	fieldDesc protoreflect.FieldDescriptor,
	fieldRules *validate.FieldRules,
	forItems bool,
) (rules protoreflect.Message, fieldRule protoreflect.FieldDescriptor, done bool, err error) {
	refRules := fieldRules.ProtoReflect()
	setOneof := refRules.WhichOneof(fieldRulesOneofDesc)
	if setOneof == nil {
		return nil, nil, true, nil
	}
	expected, ok := c.getExpectedRuleDescriptor(fieldDesc, forItems)
	if ok && setOneof.FullName() != expected.FullName() {
		return nil, nil, true, &CompilationError{cause: fmt.Errorf(
			"expected rule %q, got %q on field %q",
			expected.FullName(),
			setOneof.FullName(),
			fieldDesc.FullName(),
		)}
	}

	if !ok {
		// The only expected rule descriptor for message fields is for well known types.
		// If we didn't find a descriptor and this is a message, there must be a mismatch.
		if fieldDesc.Kind() == protoreflect.MessageKind {
			return nil, nil, true, &CompilationError{cause: fmt.Errorf(
				"mismatched message rules, %q is not a valid rule for field %q",
				setOneof.FullName(),
				fieldDesc.FullName(),
			)}
		}
		if !refRules.Has(setOneof) {
			return nil, nil, true, nil
		}
	}

	rules = refRules.Get(setOneof).Message()
	return rules, setOneof, false, nil
}

// prepareEnvironment prepares the environment for compiling standard rule
// expressions.
func (c *cache) prepareEnvironment(
	env *cel.Env,
	fieldDesc protoreflect.FieldDescriptor,
	rules protoreflect.Message,
	forItems bool,
) (*cel.Env, error) {
	env, err := extendEnv(env,
		cel.Types(rules.Interface()),
		cel.Variable("this", pvcel.ProtoFieldToType(fieldDesc, true, forItems)),
		cel.Variable("rules",
			cel.ObjectType(string(rules.Descriptor().FullName()))),
	)
	if err != nil {
		return nil, &CompilationError{cause: fmt.Errorf(
			"failed to extend base environment: %w", err)}
	}
	return env, nil
}

// loadOrCompileStandardRule loads the precompiled ASTs for the
// specified rule field from the Cache if present or precomputes them
// otherwise. The result may be empty if the rule does not have associated
// CEL expressions.
func (c *cache) loadOrCompileStandardRule(
	env *cel.Env,
	setOneOf protoreflect.FieldDescriptor,
	ruleFieldDesc protoreflect.FieldDescriptor,
) (set astSet, err error) {
	if cachedRule, ok := c.cache[ruleFieldDesc]; ok {
		return cachedRule, nil
	}
	predefinedRules, _ := ResolvePredefinedRules(
		ruleFieldDesc,
	)
	exprs := expressions{
		Rules: predefinedRules.GetCel(),
		RulePath: []*validate.FieldPathElement{
			fieldPathElement(setOneOf),
			fieldPathElement(ruleFieldDesc),
		},
	}
	set, err = compileASTs(exprs, env)
	if err != nil {
		return set, &CompilationError{cause: fmt.Errorf(
			"failed to compile standard rule %q: %w",
			ruleFieldDesc.FullName(), err)}
	}
	c.cache[ruleFieldDesc] = set
	return set, nil
}

// getExpectedRuleDescriptor produces the field descriptor from the
// validate.FieldRules 'type' oneof that matches the provided target
// field descriptor. If ok is false, the field does not expect any standard
// rules.
func (c *cache) getExpectedRuleDescriptor(
	targetFieldDesc protoreflect.FieldDescriptor,
	forItems bool,
) (expected protoreflect.FieldDescriptor, ok bool) {
	switch {
	case targetFieldDesc.IsMap():
		return mapFieldRulesDesc, true
	case targetFieldDesc.IsList() && !forItems:
		return repeatedFieldRulesDesc, true
	case targetFieldDesc.Kind() == protoreflect.MessageKind,
		targetFieldDesc.Kind() == protoreflect.GroupKind:
		expected, ok = expectedWKTRules[targetFieldDesc.Message().FullName()]
		return expected, ok
	default:
		expected, ok = expectedStandardRules[targetFieldDesc.Kind()]
		return expected, ok
	}
}

func reparseUnrecognized(
	extensionTypeResolver protoregistry.ExtensionTypeResolver,
	reflectMessage protoreflect.Message,
) error {
	if unknown := reflectMessage.GetUnknown(); len(unknown) > 0 {
		reflectMessage.SetUnknown(nil)
		options := proto.UnmarshalOptions{
			Resolver: extensionTypeResolver,
			Merge:    true,
		}
		if err := options.Unmarshal(unknown, reflectMessage.Interface()); err != nil {
			return err
		}
	}
	return nil
}
