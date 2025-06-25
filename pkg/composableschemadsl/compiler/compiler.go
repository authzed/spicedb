package compiler

import (
	"errors"
	"fmt"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/composableschemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
	"github.com/authzed/spicedb/pkg/composableschemadsl/parser"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// InputSchema defines the input for a Compile.
type InputSchema struct {
	// Source is the source of the schema being compiled.
	Source input.Source

	// Schema is the contents being compiled.
	SchemaString string
}

// SchemaDefinition represents an object or caveat definition in a schema.
type SchemaDefinition interface {
	proto.Message

	GetName() string
}

// CompiledSchema is the result of compiling a schema when there are no errors.
type CompiledSchema struct {
	// ObjectDefinitions holds the object definitions in the schema.
	ObjectDefinitions []*core.NamespaceDefinition

	// CaveatDefinitions holds the caveat definitions in the schema.
	CaveatDefinitions []*core.CaveatDefinition

	// OrderedDefinitions holds the object and caveat definitions in the schema, in the
	// order in which they were found.
	OrderedDefinitions []SchemaDefinition

	rootNode *dslNode
	mapper   input.PositionMapper
}

// SourcePositionToRunePosition converts a source position to a rune position.
func (cs CompiledSchema) SourcePositionToRunePosition(source input.Source, position input.Position) (int, error) {
	return cs.mapper.LineAndColToRunePosition(position.LineNumber, position.ColumnPosition, source)
}

type config struct {
	skipValidation   bool
	objectTypePrefix *string
	allowedFlags     []string
	caveatTypeSet    *caveattypes.TypeSet

	// In an import context, this is the folder containing
	// the importing schema (as opposed to imported schemas)
	sourceFolder string
}

func SkipValidation() Option { return func(cfg *config) { cfg.skipValidation = true } }

// Config for the prefix attached to all definitions, such as in Serverless
// where it's `someorganization/` in front of each definition.
func ObjectTypePrefix(prefix string) ObjectPrefixOption {
	return func(cfg *config) { cfg.objectTypePrefix = &prefix }
}

// Config that does not supply the prefix but requires the prefix on all objects.
func RequirePrefixedObjectType() ObjectPrefixOption {
	return func(cfg *config) { cfg.objectTypePrefix = nil }
}

// Config that allows for no prefix. This is the "normal" default.
func AllowUnprefixedObjectType() ObjectPrefixOption {
	return func(cfg *config) { cfg.objectTypePrefix = new(string) }
}

// WithCaveatTypeSet sets the caveat type set to use for the compilation.
func WithCaveatTypeSet(caveatTypeSet *caveattypes.TypeSet) Option {
	return func(cfg *config) { cfg.caveatTypeSet = caveatTypeSet }
}

const expirationFlag = "expiration"

func DisallowExpirationFlag() Option {
	return func(cfg *config) {
		cfg.allowedFlags = lo.Filter(cfg.allowedFlags, func(s string, _ int) bool {
			return s != expirationFlag
		})
	}
}

// Config that supplies the root source folder for compilation. Required
// for relative import syntax to work properly.
func SourceFolder(sourceFolder string) Option {
	return func(cfg *config) { cfg.sourceFolder = sourceFolder }
}

type Option func(*config)

type ObjectPrefixOption func(*config)

// Compile compilers the input schema into a set of namespace definition protos.
func Compile(schema InputSchema, prefix ObjectPrefixOption, opts ...Option) (*CompiledSchema, error) {
	cfg := &config{
		allowedFlags: make([]string, 0, 1),
	}

	// Enable `expiration` flag by default.
	cfg.allowedFlags = append(cfg.allowedFlags, expirationFlag)

	prefix(cfg) // required option

	for _, fn := range opts {
		fn(cfg)
	}

	root, mapper, err := parseSchema(schema)
	if err != nil {
		return nil, err
	}

	// NOTE: import translation is done separately so that partial references
	// and definitions defined in separate files can correctly resolve.
	err = translateImports(importResolutionContext{
		globallyVisitedFiles: mapz.NewSet[string](),
		locallyVisitedFiles:  mapz.NewSet[string](),
		sourceFolder:         cfg.sourceFolder,
		mapper:               mapper,
	}, root)
	if err != nil {
		return nil, err
	}

	initialCompiledPartials := make(map[string][]*core.Relation)
	caveatTypeSet := caveattypes.TypeSetOrDefault(cfg.caveatTypeSet)
	compiled, err := translate(&translationContext{
		objectTypePrefix:   cfg.objectTypePrefix,
		mapper:             mapper,
		schemaString:       schema.SchemaString,
		skipValidate:       cfg.skipValidation,
		allowedFlags:       cfg.allowedFlags,
		existingNames:      mapz.NewSet[string](),
		compiledPartials:   initialCompiledPartials,
		unresolvedPartials: mapz.NewMultiMap[string, *dslNode](),
		caveatTypeSet:      caveatTypeSet,
	}, root)
	if err != nil {
		var withNodeError withNodeError
		if errors.As(err, &withNodeError) {
			err = toContextError(withNodeError.Error(), withNodeError.errorSourceCode, withNodeError.node, mapper)
		}

		return nil, err
	}

	return compiled, nil
}

func parseSchema(schema InputSchema) (*dslNode, input.PositionMapper, error) {
	mapper := newPositionMapper(schema)
	root := parser.Parse(createAstNode, schema.Source, schema.SchemaString).(*dslNode)
	errs := root.FindAll(dslshape.NodeTypeError)
	if len(errs) > 0 {
		err := errorNodeToError(errs[0], mapper)
		return nil, nil, err
	}
	return root, mapper, nil
}

func errorNodeToError(node *dslNode, mapper input.PositionMapper) error {
	if node.GetType() != dslshape.NodeTypeError {
		return fmt.Errorf("given none error node")
	}

	errMessage, err := node.GetString(dslshape.NodePredicateErrorMessage)
	if err != nil {
		return fmt.Errorf("could not get error message for error node: %w", err)
	}

	errorSourceCode := ""
	if node.Has(dslshape.NodePredicateErrorSource) {
		es, err := node.GetString(dslshape.NodePredicateErrorSource)
		if err != nil {
			return fmt.Errorf("could not get error source for error node: %w", err)
		}

		errorSourceCode = es
	}

	return toContextError(errMessage, errorSourceCode, node, mapper)
}

func toContextError(errMessage string, errorSourceCode string, node *dslNode, mapper input.PositionMapper) error {
	sourceRange, err := node.Range(mapper)
	if err != nil {
		return fmt.Errorf("could not get range for error node: %w", err)
	}

	formattedRange, err := formatRange(sourceRange)
	if err != nil {
		return err
	}

	source, err := node.GetString(dslshape.NodePredicateSource)
	if err != nil {
		return fmt.Errorf("missing source for node: %w", err)
	}

	return WithContextError{
		BaseCompilerError: BaseCompilerError{
			error:       fmt.Errorf("parse error in %s: %s", formattedRange, errMessage),
			BaseMessage: errMessage,
		},
		SourceRange:     sourceRange,
		Source:          input.Source(source),
		ErrorSourceCode: errorSourceCode,
	}
}

func formatRange(rnge input.SourceRange) (string, error) {
	startLine, startCol, err := rnge.Start().LineAndColumn()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("`%s`, line %v, column %v", rnge.Source(), startLine+1, startCol+1), nil
}
