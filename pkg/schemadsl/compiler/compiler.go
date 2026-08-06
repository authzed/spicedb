package compiler

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"google.golang.org/protobuf/proto"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/decorators"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/schemadsl/lexer"
	"github.com/authzed/spicedb/pkg/schemadsl/parser"
)

// InputSchema defines the input for a Compile.
type InputSchema struct {
	// Source is the source of the schema being compiled.
	// It may be a simple string like "schema" or a full path like "file://something/root.zed"
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
	skipValidation    bool
	objectTypePrefix  *string
	allowedFlags      *mapz.Set[string]
	caveatTypeSet     *caveattypes.TypeSet
	decoratorRegistry decorators.Registry

	// In an import context, this is the FS containing
	// the importing schema (as opposed to imported schemas)
	sourceFS fs.FS
}

func SkipValidation() Option { return func(cfg *config) { cfg.skipValidation = true } }

func ObjectTypePrefix(prefix string) ObjectPrefixOption {
	return func(cfg *config) { cfg.objectTypePrefix = &prefix }
}

func RequirePrefixedObjectType() ObjectPrefixOption {
	return func(cfg *config) { cfg.objectTypePrefix = nil }
}

func AllowUnprefixedObjectType() ObjectPrefixOption {
	return func(cfg *config) { cfg.objectTypePrefix = new(string) }
}

func CaveatTypeSet(cts *caveattypes.TypeSet) Option {
	return func(cfg *config) { cfg.caveatTypeSet = cts }
}

// WithDecoratorRegistry sets the registry used to validate decorators. Defaults to
// decorators.DefaultRegistry.
func WithDecoratorRegistry(r decorators.Registry) Option {
	return func(cfg *config) { cfg.decoratorRegistry = r }
}

// Config that supplies the root source folder for compilation. Required
// for relative import syntax to work properly.
func SourceFolder(sourceFolder string) Option {
	return func(cfg *config) { cfg.sourceFS = os.DirFS(sourceFolder) }
}

// Config that supplies the fs.FS for compilation as an alternative to
// SourceFolder.
func SourceFS(fsys fs.FS) Option {
	return func(cfg *config) { cfg.sourceFS = fsys }
}

const (
	expirationFlag   = "expiration"
	selfFlag         = "self"
	typeCheckingFlag = "typechecking"
	partialFlag      = "partial"
	importFlag       = "import"
)

// allowedFlags builds the default set of `use` flags a schema may declare from the lexer's
// own registry (lexer.AllUseFlags), rather than maintaining a second, independent list here.
// This keeps the two in sync: whatever the lexer recognizes as a flag is allowed by default,
// and callers use the Disallow* options to remove specific flags for their deployment. In a
// production binary this yields exactly the same five flags as before (expiration, self,
// typechecking, partial, import); in test binaries it additionally includes the decorators
// test flag registered by lexer/flags.go, which is required for that flag to compile at all.
func allowedFlags() *mapz.Set[string] {
	return mapz.NewSet(lexer.AllUseFlags...)
}

func DisallowExpirationFlag() Option {
	return func(cfg *config) {
		cfg.allowedFlags.Delete(expirationFlag)
	}
}

func DisallowImportFlag() Option {
	return func(cfg *config) {
		cfg.allowedFlags.Delete(importFlag)
	}
}

// DisallowFlags removes the named `use` flags from the set a schema may declare.
func DisallowFlags(names ...string) Option {
	return func(cfg *config) {
		for _, name := range names {
			cfg.allowedFlags.Delete(name)
		}
	}
}

type Option func(*config)

type ObjectPrefixOption func(*config)

// Compile compilers the input schema into a set of namespace definition protos.
func Compile(schema InputSchema, prefix ObjectPrefixOption, opts ...Option) (*CompiledSchema, error) {
	cfg := &config{
		allowedFlags:      allowedFlags(),
		decoratorRegistry: decorators.DefaultRegistry,
	}

	prefix(cfg) // required option

	for _, fn := range opts {
		fn(cfg)
	}

	root, mapper, err := parseSchema(schema)
	if err != nil {
		return nil, err
	}

	present, err := validateImportPresence(cfg.allowedFlags.Has(importFlag), root)
	if err != nil {
		// This condition should basically always be satisfied (we trigger errors off of the node),
		// but we're defensive here in case the implementation changes.
		var withNodeError withNodeError
		if errors.As(err, &withNodeError) {
			return nil, toContextError(withNodeError.Error(), withNodeError.errorSourceCode, withNodeError.node, mapper)
		}
		return nil, err
	}

	if present {
		// NOTE: import translation is done separately so that partial references
		// and definitions defined in separate files can correctly resolve.
		irc, err := newImportResolutionContext(cfg.sourceFS, mapper, "")
		if err != nil {
			return nil, err
		}
		err = irc.translateImports(root, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	initialCompiledPartials := make(map[string][]*core.Relation)
	caveatTypeSet := caveattypes.TypeSetOrDefault(cfg.caveatTypeSet)
	compiled, err := translate(&translationContext{
		objectTypePrefix:   cfg.objectTypePrefix,
		mapper:             mapper,
		skipValidate:       cfg.skipValidation,
		allowedFlags:       cfg.allowedFlags,
		enabledFlags:       mapz.NewSet[string](),
		existingNames:      mapz.NewSet[string](),
		compiledPartials:   initialCompiledPartials,
		partialDecorators:  make(map[string][]*core.Decorator),
		unresolvedPartials: mapz.NewMultiMap[string, *dslNode](),
		caveatTypeSet:      caveatTypeSet,
		decoratorRegistry:  cfg.decoratorRegistry,
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

// validateImportPresence validates whether a given AST is valid based on whether
// imports are allowed in the context. if they're present and disallowed it returns
// a validation error; otherwise it returns the presence.
func validateImportPresence(allowed bool, root *dslNode) (present bool, err error) {
	present = false
	for _, topLevelNode := range root.GetChildren() {
		// Process import nodes; ignore the others
		if topLevelNode.GetType() == dslshape.NodeTypeImport {
			if !allowed {
				return false, topLevelNode.Errorf("import statements are not allowed in this context")
			}
			present = true
		}
	}
	return present, nil
}

func errorNodeToError(node *dslNode, mapper input.PositionMapper) error {
	if node.GetType() != dslshape.NodeTypeError {
		return errors.New("given none error node")
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
