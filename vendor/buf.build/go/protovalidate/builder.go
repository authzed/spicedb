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
	"cmp"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	pvcel "buf.build/go/protovalidate/cel"
	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

//nolint:gochecknoglobals
var (
	celExpressionDescriptor = (&validate.FieldRules{}).ProtoReflect().Descriptor().Fields().ByName("cel_expression")
	celExpressionField      = fieldPathElement(celExpressionDescriptor)
	celRuleDescriptor       = (&validate.FieldRules{}).ProtoReflect().Descriptor().Fields().ByName("cel")
	celRuleField            = fieldPathElement(celRuleDescriptor)
)

// builder is a build-through cache of message evaluators keyed off the provided
// descriptor.
type builder struct {
	mtx                   sync.Mutex                   // serializes cache writes.
	cache                 atomic.Pointer[messageCache] // copy-on-write cache.
	env                   *cel.Env
	rules                 cache
	extensionTypeResolver protoregistry.ExtensionTypeResolver
	allowUnknownFields    bool
	Load                  func(desc protoreflect.MessageDescriptor) messageEvaluator
}

// newBuilder initializes a new Builder.
func newBuilder(
	env *cel.Env,
	disableLazy bool,
	extensionTypeResolver protoregistry.ExtensionTypeResolver,
	allowUnknownFields bool,
	seedDesc ...protoreflect.MessageDescriptor,
) *builder {
	bldr := &builder{
		env:                   env,
		rules:                 newCache(),
		extensionTypeResolver: extensionTypeResolver,
		allowUnknownFields:    allowUnknownFields,
	}

	if disableLazy {
		bldr.Load = bldr.load
	} else {
		bldr.Load = bldr.loadOrBuild
	}

	cache := make(messageCache, len(seedDesc))
	for _, desc := range seedDesc {
		bldr.build(desc, cache)
	}
	bldr.cache.Store(&cache)
	return bldr
}

// load returns a pre-cached MessageEvaluator for the given descriptor or, if
// the descriptor is unknown, returns an evaluator that always resolves to a
// errors.CompilationError.
func (bldr *builder) load(desc protoreflect.MessageDescriptor) messageEvaluator {
	if eval, ok := (*bldr.cache.Load())[desc]; ok {
		return eval
	}
	return unknownMessage{desc: desc}
}

// loadOrBuild either returns a memoized MessageEvaluator for the given
// descriptor or lazily constructs a new one. This method is thread-safe via
// locking.
func (bldr *builder) loadOrBuild(desc protoreflect.MessageDescriptor) messageEvaluator {
	if eval, ok := (*bldr.cache.Load())[desc]; ok {
		return eval
	}
	bldr.mtx.Lock()
	defer bldr.mtx.Unlock()
	cache := *bldr.cache.Load()
	if eval, ok := cache[desc]; ok {
		return eval
	}
	newCache := cache.Clone()
	msgEval := bldr.build(desc, newCache)
	bldr.cache.Store(&newCache)
	return msgEval
}

func (bldr *builder) build(
	desc protoreflect.MessageDescriptor,
	cache messageCache,
) *message {
	if eval, ok := cache[desc]; ok {
		return eval
	}
	msgEval := &message{}
	cache[desc] = msgEval
	bldr.buildMessage(desc, msgEval, cache)
	return msgEval
}

func (bldr *builder) buildMessage(
	desc protoreflect.MessageDescriptor, msgEval *message,
	cache messageCache,
) {
	msgRules, _ := ResolveMessageRules(desc)

	steps := []func(
		desc protoreflect.MessageDescriptor,
		msgRules *validate.MessageRules,
		msg *message,
		cache messageCache,
	){
		bldr.processMessageExpressions,
		bldr.processMessageOneofRules,
		bldr.processOneofRules,
		bldr.processFields,
	}

	for _, step := range steps {
		step(desc, msgRules, msgEval, cache)
	}
}

func (bldr *builder) processMessageExpressions(
	desc protoreflect.MessageDescriptor,
	msgRules *validate.MessageRules,
	msgEval *message,
	_ messageCache,
) {
	exprs := expressions{
		Rules: append(expressionsToRules(msgRules.GetCelExpression()), msgRules.GetCel()...),
	}
	compiledExprs, err := compile(
		exprs,
		bldr.env,
		cel.Types(dynamicpb.NewMessage(desc)),
		cel.Variable("this", cel.ObjectType(string(desc.FullName()))),
	)
	if err != nil {
		msgEval.Err = err
		return
	}

	msgEval.Append(celPrograms{programSet: compiledExprs})
}

func (bldr *builder) processMessageOneofRules(
	desc protoreflect.MessageDescriptor,
	msgRules *validate.MessageRules,
	msgEval *message,
	_ messageCache,
) {
	oneofRules := msgRules.GetOneof()
	for _, rule := range oneofRules {
		fields := rule.GetFields()
		if len(fields) == 0 {
			msgEval.Err = &CompilationError{
				cause: fmt.Errorf("at least one field must be specified in oneof rule for the message %s", desc.FullName()),
			}
			return
		}
		seen := make(map[string]struct{}, len(fields))
		fdescs := make([]protoreflect.FieldDescriptor, 0, len(fields))
		for _, name := range fields {
			if _, ok := seen[name]; ok {
				msgEval.Err = &CompilationError{
					cause: fmt.Errorf("duplicate %s in oneof rule for the message %s", name, desc.FullName()),
				}
				return
			}
			seen[name] = struct{}{}
			fdesc := desc.Fields().ByName(protoreflect.Name(name))
			if fdesc == nil {
				msgEval.Err = &CompilationError{
					cause: fmt.Errorf("field %s not found in message %s", name, desc.FullName()),
				}
				return
			}
			fdescs = append(fdescs, fdesc)
		}
		oneofEval := &messageOneof{
			Fields:   fdescs,
			Required: rule.GetRequired(),
		}
		msgEval.AppendNested(oneofEval)
	}
}

func (bldr *builder) processOneofRules(
	desc protoreflect.MessageDescriptor,
	_ *validate.MessageRules,
	msgEval *message,
	_ messageCache,
) {
	oneofs := desc.Oneofs()
	for i := range oneofs.Len() {
		oneofDesc := oneofs.Get(i)
		oneofRules, _ := ResolveOneofRules(oneofDesc)
		oneofEval := oneof{
			Descriptor: oneofDesc,
			Required:   oneofRules.GetRequired(),
		}
		msgEval.AppendNested(oneofEval)
	}
}

func (bldr *builder) processFields(
	desc protoreflect.MessageDescriptor,
	msgRules *validate.MessageRules,
	msgEval *message,
	cache messageCache,
) {
	fields := desc.Fields()
	for i := range fields.Len() {
		fdesc := fields.Get(i)
		fieldRules, _ := ResolveFieldRules(fdesc)
		fldEval, err := bldr.buildField(fdesc, fieldRules, msgRules, cache)
		if err != nil {
			fldEval.Err = err
		}
		msgEval.AppendNested(fldEval)
	}
}

func (bldr *builder) buildField(
	fieldDescriptor protoreflect.FieldDescriptor,
	fieldRules *validate.FieldRules,
	msgRules *validate.MessageRules,
	cache messageCache,
) (field, error) {
	if fieldRules != nil && !fieldRules.HasIgnore() && isPartOfMessageOneof(msgRules, fieldDescriptor) {
		fieldRules = proto.CloneOf(fieldRules)
		fieldRules.SetIgnore(validate.Ignore_IGNORE_IF_ZERO_VALUE)
	}
	fld := field{
		Value: value{
			Descriptor: fieldDescriptor,
		},
		HasPresence: fieldDescriptor.HasPresence(),
		Required:    fieldRules.GetRequired(),
		Ignore:      fieldRules.GetIgnore(),
	}
	err := bldr.buildValue(fieldDescriptor, fieldRules, &fld.Value, cache)
	return fld, err
}

func (bldr *builder) buildValue(
	fdesc protoreflect.FieldDescriptor,
	rules *validate.FieldRules,
	valEval *value,
	cache messageCache,
) (err error) {
	if bldr.shouldIgnoreAlways(rules) {
		return nil
	}

	steps := []func(
		fdesc protoreflect.FieldDescriptor,
		fieldRules *validate.FieldRules,
		valEval *value,
		cache messageCache,
	) error{
		bldr.processIgnoreEmpty,
		bldr.processFieldExpressions,
		bldr.processEmbeddedMessage,
		bldr.processWrapperRules,
		bldr.processStandardRules,
		bldr.processAnyRules,
		bldr.processEnumRules,
		bldr.processMapRules,
		bldr.processRepeatedRules,
	}

	for _, step := range steps {
		if err = step(fdesc, rules, valEval, cache); err != nil {
			return err
		}
	}
	return nil
}

func (bldr *builder) processIgnoreEmpty(
	fdesc protoreflect.FieldDescriptor,
	rules *validate.FieldRules,
	val *value,
	_ messageCache,
) error {
	// the only time we need to ignore empty on a value is if it's evaluating a
	// field item (repeated element or map key/value).
	val.IgnoreEmpty = val.NestedRule != nil && bldr.shouldIgnoreEmpty(rules)
	if val.IgnoreEmpty {
		val.Zero = bldr.zeroValue(fdesc, val.NestedRule != nil)
	}
	return nil
}

func (bldr *builder) processFieldExpressions(
	fieldDesc protoreflect.FieldDescriptor,
	fieldRules *validate.FieldRules,
	eval *value,
	_ messageCache,
) error {
	celTyp := pvcel.ProtoFieldToType(fieldDesc, false, eval.NestedRule != nil)
	opts := append(
		pvcel.RequiredEnvOptions(fieldDesc),
		cel.Variable("this", celTyp),
	)
	compileWithPath := func(exprs expressions, fieldPathElement *validate.FieldPathElement, descriptor protoreflect.FieldDescriptor) (set programSet, err error) {
		set, err = compile(exprs, bldr.env, opts...)
		if err != nil {
			return set, err
		}
		for i := range set.programs {
			set.programs[i].Path = []*validate.FieldPathElement{
				validate.FieldPathElement_builder{
					FieldNumber: proto.Int32(fieldPathElement.GetFieldNumber()),
					FieldType:   fieldPathElement.GetFieldType().Enum(),
					FieldName:   proto.String(fieldPathElement.GetFieldName()),
					Index:       proto.Uint64(uint64(i)), //nolint:gosec // indices are guaranteed to be non-negative
				}.Build(),
			}
			set.programs[i].Descriptor = descriptor
		}
		return set, nil
	}
	compiledExpressions, err := compileWithPath(
		expressions{
			Rules: expressionsToRules(fieldRules.GetCelExpression()),
		},
		celExpressionField,
		celExpressionDescriptor,
	)
	if err != nil {
		return err
	}
	celRuleCompiledExpressions, err := compileWithPath(
		expressions{
			Rules: fieldRules.GetCel(),
		},
		celRuleField,
		celRuleDescriptor,
	)
	if err != nil {
		return err
	}
	// Merge the two program sets. If cel rules were compiled, use their env
	// since it has all the types registered.
	compiledExpressions.programs = append(compiledExpressions.programs, celRuleCompiledExpressions.programs...)
	if len(compiledExpressions.programs) > 0 {
		compiledExpressions.env = cmp.Or(compiledExpressions.env, celRuleCompiledExpressions.env, bldr.env)
		eval.Rules = append(eval.Rules,
			celPrograms{
				base:       newBase(eval),
				programSet: compiledExpressions,
			},
		)
	}
	return nil
}

func (bldr *builder) processEmbeddedMessage(
	fdesc protoreflect.FieldDescriptor,
	_ *validate.FieldRules,
	valEval *value,
	cache messageCache,
) error {
	if !isMessageField(fdesc) ||
		fdesc.IsMap() ||
		(fdesc.IsList() && valEval.NestedRule == nil) {
		return nil
	}

	embedEval := bldr.build(fdesc.Message(), cache)
	if err := embedEval.Err; err != nil {
		return &CompilationError{cause: fmt.Errorf(
			"failed to compile embedded type %s for %s: %w",
			fdesc.Message().FullName(), fdesc.FullName(), err)}
	}
	valEval.AppendNested(&embeddedMessage{
		base:    newBase(valEval),
		message: embedEval,
	})

	return nil
}

func (bldr *builder) processWrapperRules(
	fdesc protoreflect.FieldDescriptor,
	rules *validate.FieldRules,
	valEval *value,
	cache messageCache,
) error {
	if !isMessageField(fdesc) ||
		fdesc.IsMap() ||
		(fdesc.IsList() && valEval.NestedRule == nil) {
		return nil
	}
	refRules := rules.ProtoReflect()
	setOneof := refRules.WhichOneof(fieldRulesOneofDesc)
	if setOneof == nil {
		return nil
	}

	expectedWrapperDescriptor, ok := expectedWrapperRules(fdesc.Message().FullName())
	if ok && setOneof.FullName() != expectedWrapperDescriptor.FullName() {
		return &CompilationError{cause: fmt.Errorf(
			"expected rule %q, got %q on field %q",
			expectedWrapperDescriptor.FullName(),
			setOneof.FullName(),
			fdesc.FullName(),
		)}
	}

	if !ok || !rules.ProtoReflect().Has(expectedWrapperDescriptor) {
		return nil
	}
	unwrapped := value{
		Descriptor: valEval.Descriptor,
		NestedRule: valEval.NestedRule,
	}
	err := bldr.buildValue(fdesc.Message().Fields().ByName("value"), rules, &unwrapped, cache)
	if err != nil {
		return err
	}
	valEval.Append(unwrapped.Rules)
	return nil
}

func (bldr *builder) processStandardRules(
	fdesc protoreflect.FieldDescriptor,
	rules *validate.FieldRules,
	valEval *value,
	_ messageCache,
) error {
	// If this is a wrapper field, just return. Wrapper fields are handled by
	// processWrapperRules and their wrapped values are passed through the process gauntlet.
	if isMessageField(fdesc) {
		if _, ok := expectedWrapperRules(fdesc.Message().FullName()); ok {
			return nil
		}
	}

	stdRules, err := bldr.rules.Build(
		bldr.env,
		fdesc,
		rules,
		bldr.extensionTypeResolver,
		bldr.allowUnknownFields,
		valEval.NestedRule != nil,
	)
	if err != nil {
		return err
	}
	valEval.Append(celPrograms{
		base:       newBase(valEval),
		programSet: stdRules,
	})
	return nil
}

func (bldr *builder) processAnyRules(
	fdesc protoreflect.FieldDescriptor,
	fieldRules *validate.FieldRules,
	valEval *value,
	_ messageCache,
) error {
	if (fdesc.IsList() && valEval.NestedRule == nil) ||
		!isMessageField(fdesc) ||
		fdesc.Message().FullName() != "google.protobuf.Any" {
		return nil
	}

	typeURLDesc := fdesc.Message().Fields().ByName("type_url")
	anyPbDesc := (&validate.AnyRules{}).ProtoReflect().Descriptor()
	inField := anyPbDesc.Fields().ByName("in")
	notInField := anyPbDesc.Fields().ByName("not_in")
	anyEval := anyPB{
		base:              newBase(valEval),
		TypeURLDescriptor: typeURLDesc,
		In:                stringsToSet(fieldRules.GetAny().GetIn()),
		NotIn:             stringsToSet(fieldRules.GetAny().GetNotIn()),
		InValue:           fieldRules.GetAny().ProtoReflect().Get(inField),
		NotInValue:        fieldRules.GetAny().ProtoReflect().Get(notInField),
	}
	valEval.Append(anyEval)
	return nil
}

func (bldr *builder) processEnumRules(
	fdesc protoreflect.FieldDescriptor,
	fieldRules *validate.FieldRules,
	valEval *value,
	_ messageCache,
) error {
	if fdesc.Kind() != protoreflect.EnumKind {
		return nil
	}
	if fieldRules.GetEnum().GetDefinedOnly() {
		valEval.Append(definedEnum{
			base:             newBase(valEval),
			ValueDescriptors: fdesc.Enum().Values(),
		})
	}
	return nil
}

func (bldr *builder) processMapRules(
	fieldDesc protoreflect.FieldDescriptor,
	rules *validate.FieldRules,
	valEval *value,
	cache messageCache,
) error {
	if !fieldDesc.IsMap() {
		return nil
	}

	mapEval := newKVPairs(valEval)

	err := bldr.buildValue(
		fieldDesc.MapKey(),
		rules.GetMap().GetKeys(),
		&mapEval.KeyRules,
		cache)
	if err != nil {
		return &CompilationError{cause: fmt.Errorf(
			"failed to compile key rules for map %s: %w",
			fieldDesc.FullName(), err)}
	}

	err = bldr.buildValue(
		fieldDesc.MapValue(),
		rules.GetMap().GetValues(),
		&mapEval.ValueRules,
		cache)
	if err != nil {
		return &CompilationError{cause: fmt.Errorf(
			"failed to compile value rules for map %s: %w",
			fieldDesc.FullName(), err)}
	}

	valEval.Append(mapEval)
	return nil
}

func (bldr *builder) processRepeatedRules(
	fdesc protoreflect.FieldDescriptor,
	fieldRules *validate.FieldRules,
	valEval *value,
	cache messageCache,
) error {
	if !fdesc.IsList() || valEval.NestedRule != nil {
		return nil
	}

	listEval := newListItems(valEval)

	err := bldr.buildValue(fdesc, fieldRules.GetRepeated().GetItems(), &listEval.ItemRules, cache)
	if err != nil {
		return &CompilationError{cause: fmt.Errorf(
			"failed to compile items rules for repeated %v: %w", fdesc.FullName(), err)}
	}

	valEval.Append(listEval)
	return nil
}

func (bldr *builder) shouldIgnoreAlways(rules *validate.FieldRules) bool {
	return rules.GetIgnore() == validate.Ignore_IGNORE_ALWAYS
}

func (bldr *builder) shouldIgnoreEmpty(rules *validate.FieldRules) bool {
	return rules.GetIgnore() == validate.Ignore_IGNORE_IF_ZERO_VALUE
}

func (bldr *builder) zeroValue(fdesc protoreflect.FieldDescriptor, forItems bool) protoreflect.Value {
	switch {
	case forItems && fdesc.IsList():
		msg := dynamicpb.NewMessage(fdesc.ContainingMessage())
		return msg.Get(fdesc).List().NewElement()
	case isMessageField(fdesc) &&
		fdesc.Cardinality() != protoreflect.Repeated:
		msg := dynamicpb.NewMessage(fdesc.Message())
		return protoreflect.ValueOfMessage(msg)
	default:
		return fdesc.Default()
	}
}

type messageCache map[protoreflect.MessageDescriptor]*message

func (c messageCache) Clone() messageCache {
	newCache := make(messageCache, len(c)+1)
	c.SyncTo(newCache)
	return newCache
}
func (c messageCache) SyncTo(other messageCache) {
	maps.Copy(other, c)
}

// isMessageField returns true if the field descriptor fdesc describes a field
// containing a submessage. Although they are represented differently on the
// wire, group fields are treated like message fields in protoreflect and have
// similar properties. In the 2023 edition of protobuf, message fields with the
// delimited encoding feature will be detected as groups, but should otherwise
// be treated the same.
func isMessageField(fdesc protoreflect.FieldDescriptor) bool {
	return fdesc.Kind() == protoreflect.MessageKind ||
		fdesc.Kind() == protoreflect.GroupKind
}

func isPartOfMessageOneof(msgRules *validate.MessageRules, field protoreflect.FieldDescriptor) bool {
	return slices.ContainsFunc(msgRules.GetOneof(), func(oneof *validate.MessageOneofRule) bool {
		return slices.Contains(oneof.GetFields(), string(field.Name()))
	})
}

func expressionsToRules(expressions []string) []*validate.Rule {
	rules := make([]*validate.Rule, 0, len(expressions))
	for _, expr := range expressions {
		rules = append(rules, validate.Rule_builder{
			Id:         proto.String(expr),
			Expression: proto.String(expr),
		}.Build())
	}
	return rules
}
