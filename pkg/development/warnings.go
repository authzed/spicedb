package development

import (
	"context"
	"fmt"

	"github.com/ccoveille/go-safecast"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var allChecks = checks{
	relationChecks: []relationCheck{
		lintRelationReferencesParentType,
	},
	computedUsersetChecks: []computedUsersetCheck{
		lintPermissionReferencingItself,
	},
	ttuChecks: []ttuCheck{
		lintArrowReferencingRelation,
		lintArrowReferencingUnreachable,
		lintArrowOverSubRelation,
	},
}

func warningForMetadata(warningName string, message string, sourceCode string, metadata namespace.WithSourcePosition) *devinterface.DeveloperWarning {
	return warningForPosition(warningName, message, sourceCode, metadata.GetSourcePosition())
}

func warningForPosition(warningName string, message string, sourceCode string, sourcePosition *corev1.SourcePosition) *devinterface.DeveloperWarning {
	if sourcePosition == nil {
		return &devinterface.DeveloperWarning{
			Message:    message,
			SourceCode: sourceCode,
		}
	}

	// NOTE: zeroes on failure are fine here.
	lineNumber, err := safecast.ToUint32(sourcePosition.ZeroIndexedLineNumber)
	if err != nil {
		log.Err(err).Msg("could not cast lineNumber to uint32")
	}
	columnNumber, err := safecast.ToUint32(sourcePosition.ZeroIndexedColumnPosition)
	if err != nil {
		log.Err(err).Msg("could not cast columnPosition to uint32")
	}

	return &devinterface.DeveloperWarning{
		Message:    message + " (" + warningName + ")",
		Line:       lineNumber + 1,
		Column:     columnNumber + 1,
		SourceCode: sourceCode,
	}
}

// GetWarnings returns a list of warnings for the given developer context.
func GetWarnings(ctx context.Context, devCtx *DevContext) ([]*devinterface.DeveloperWarning, error) {
	warnings := []*devinterface.DeveloperWarning{}
	res := schema.ResolverForCompiledSchema(*devCtx.CompiledSchema)
	ts := schema.NewTypeSystem(res)

	for _, def := range devCtx.CompiledSchema.ObjectDefinitions {
		found, err := addDefinitionWarnings(ctx, def, ts)
		if err != nil {
			return nil, err
		}
		warnings = append(warnings, found...)
	}

	return warnings, nil
}

type contextKey string

var relationKey = contextKey("relation")

func addDefinitionWarnings(ctx context.Context, nsDef *corev1.NamespaceDefinition, ts *schema.TypeSystem) ([]*devinterface.DeveloperWarning, error) {
	def, err := schema.NewDefinition(ts, nsDef)
	if err != nil {
		return nil, err
	}

	warnings := []*devinterface.DeveloperWarning{}
	for _, rel := range nsDef.Relation {
		ctx = context.WithValue(ctx, relationKey, rel)

		for _, check := range allChecks.relationChecks {
			if shouldSkipCheck(rel.Metadata, check.name) {
				continue
			}

			checkerWarning, err := check.fn(ctx, rel, def)
			if err != nil {
				return nil, err
			}

			if checkerWarning != nil {
				warnings = append(warnings, checkerWarning)
			}
		}

		if def.IsPermission(rel.Name) {
			found, err := walkUsersetRewrite(ctx, rel.UsersetRewrite, rel, allChecks, def)
			if err != nil {
				return nil, err
			}

			warnings = append(warnings, found...)
		}
	}

	return warnings, nil
}

func shouldSkipCheck(metadata *corev1.Metadata, name string) bool {
	if metadata == nil {
		return false
	}

	comments := namespace.GetComments(metadata)
	for _, comment := range comments {
		if comment == "// spicedb-ignore-warning: "+name {
			return true
		}
	}

	return false
}

type tupleset interface {
	GetRelation() string
}

type ttu interface {
	GetTupleset() tupleset
	GetComputedUserset() *corev1.ComputedUserset
	GetArrowString() (string, error)
}

type (
	relationChecker        func(ctx context.Context, relation *corev1.Relation, def *schema.Definition) (*devinterface.DeveloperWarning, error)
	computedUsersetChecker func(ctx context.Context, computedUserset *corev1.ComputedUserset, sourcePosition *corev1.SourcePosition, def *schema.Definition) (*devinterface.DeveloperWarning, error)
	ttuChecker             func(ctx context.Context, ttu ttu, sourcePosition *corev1.SourcePosition, def *schema.Definition) (*devinterface.DeveloperWarning, error)
)

type relationCheck struct {
	name string
	fn   relationChecker
}

type computedUsersetCheck struct {
	name string
	fn   computedUsersetChecker
}

type ttuCheck struct {
	name string
	fn   ttuChecker
}

type checks struct {
	relationChecks        []relationCheck
	computedUsersetChecks []computedUsersetCheck
	ttuChecks             []ttuCheck
}

func walkUsersetRewrite(ctx context.Context, rewrite *corev1.UsersetRewrite, relation *corev1.Relation, checks checks, def *schema.Definition) ([]*devinterface.DeveloperWarning, error) {
	if rewrite == nil {
		return nil, nil
	}

	switch t := (rewrite.RewriteOperation).(type) {
	case *corev1.UsersetRewrite_Union:
		return walkUsersetOperations(ctx, t.Union.Child, relation, checks, def)

	case *corev1.UsersetRewrite_Intersection:
		return walkUsersetOperations(ctx, t.Intersection.Child, relation, checks, def)

	case *corev1.UsersetRewrite_Exclusion:
		return walkUsersetOperations(ctx, t.Exclusion.Child, relation, checks, def)

	default:
		return nil, spiceerrors.MustBugf("unexpected rewrite operation type %T", t)
	}
}

func walkUsersetOperations(ctx context.Context, ops []*corev1.SetOperation_Child, relation *corev1.Relation, checks checks, def *schema.Definition) ([]*devinterface.DeveloperWarning, error) {
	warnings := []*devinterface.DeveloperWarning{}
	for _, op := range ops {
		switch t := op.ChildType.(type) {
		case *corev1.SetOperation_Child_XThis:
			continue

		case *corev1.SetOperation_Child_ComputedUserset:
			for _, check := range checks.computedUsersetChecks {
				if shouldSkipCheck(relation.Metadata, check.name) {
					continue
				}

				checkerWarning, err := check.fn(ctx, t.ComputedUserset, op.SourcePosition, def)
				if err != nil {
					return nil, err
				}

				if checkerWarning != nil {
					warnings = append(warnings, checkerWarning)
				}
			}

		case *corev1.SetOperation_Child_UsersetRewrite:
			found, err := walkUsersetRewrite(ctx, t.UsersetRewrite, relation, checks, def)
			if err != nil {
				return nil, err
			}

			warnings = append(warnings, found...)

		case *corev1.SetOperation_Child_FunctionedTupleToUserset:
			for _, check := range checks.ttuChecks {
				if shouldSkipCheck(relation.Metadata, check.name) {
					continue
				}

				checkerWarning, err := check.fn(ctx, wrappedFunctionedTTU{t.FunctionedTupleToUserset}, op.SourcePosition, def)
				if err != nil {
					return nil, err
				}

				if checkerWarning != nil {
					warnings = append(warnings, checkerWarning)
				}
			}

		case *corev1.SetOperation_Child_TupleToUserset:
			for _, check := range checks.ttuChecks {
				if shouldSkipCheck(relation.Metadata, check.name) {
					continue
				}

				checkerWarning, err := check.fn(ctx, wrappedTTU{t.TupleToUserset}, op.SourcePosition, def)
				if err != nil {
					return nil, err
				}

				if checkerWarning != nil {
					warnings = append(warnings, checkerWarning)
				}
			}

		case *corev1.SetOperation_Child_XNil:
			continue

		default:
			return nil, spiceerrors.MustBugf("unexpected set operation type %T", t)
		}
	}

	return warnings, nil
}

type wrappedFunctionedTTU struct {
	*corev1.FunctionedTupleToUserset
}

func (wfttu wrappedFunctionedTTU) GetTupleset() tupleset {
	return wfttu.FunctionedTupleToUserset.GetTupleset()
}

func (wfttu wrappedFunctionedTTU) GetComputedUserset() *corev1.ComputedUserset {
	return wfttu.FunctionedTupleToUserset.GetComputedUserset()
}

func (wfttu wrappedFunctionedTTU) GetArrowString() (string, error) {
	var functionName string
	switch wfttu.Function {
	case corev1.FunctionedTupleToUserset_FUNCTION_ANY:
		functionName = "any"

	case corev1.FunctionedTupleToUserset_FUNCTION_ALL:
		functionName = "all"

	default:
		return "", spiceerrors.MustBugf("unknown function type %T", wfttu.Function)
	}

	return fmt.Sprintf("%s.%s(%s)", wfttu.GetTupleset().GetRelation(), functionName, wfttu.GetComputedUserset().GetRelation()), nil
}

type wrappedTTU struct {
	*corev1.TupleToUserset
}

func (wtu wrappedTTU) GetTupleset() tupleset {
	return wtu.TupleToUserset.GetTupleset()
}

func (wtu wrappedTTU) GetComputedUserset() *corev1.ComputedUserset {
	return wtu.TupleToUserset.GetComputedUserset()
}

func (wtu wrappedTTU) GetArrowString() (string, error) {
	arrowString := fmt.Sprintf("%s->%s", wtu.GetTupleset().GetRelation(), wtu.GetComputedUserset().GetRelation())
	return arrowString, nil
}
