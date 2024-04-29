package development

import (
	"context"

	"github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/typesystem"
)

var allChecks = checkers{
	relationCheckers: []relationChecker{
		lintRelationReferencesParentType,
	},
	computedUsersetCheckers: []computedUsersetChecker{
		lintPermissionReferencingItself,
	},
	ttuCheckers: []ttuChecker{
		lintArrowReferencingRelation,
		lintArrowReferencingUnreachable,
		lintArrowOverSubRelation,
	},
}

func warningForMetadata(message string, metadata namespace.WithSourcePosition) *devinterface.DeveloperWarning {
	if metadata.GetSourcePosition() == nil {
		return &devinterface.DeveloperWarning{
			Message: message,
		}
	}

	lineNumber := metadata.GetSourcePosition().ZeroIndexedLineNumber + 1
	columnNumber := metadata.GetSourcePosition().ZeroIndexedColumnPosition + 1

	return &devinterface.DeveloperWarning{
		Message: message,
		Line:    uint32(lineNumber),
		Column:  uint32(columnNumber),
	}
}

// GetWarnings returns a list of warnings for the given developer context.
func GetWarnings(ctx context.Context, devCtx *DevContext) ([]*devinterface.DeveloperWarning, error) {
	warnings := []*devinterface.DeveloperWarning{}
	resolver := typesystem.ResolverForSchema(*devCtx.CompiledSchema)

	for _, def := range devCtx.CompiledSchema.ObjectDefinitions {
		found, err := addDefinitionWarnings(ctx, def, resolver)
		if err != nil {
			return nil, err
		}
		warnings = append(warnings, found...)
	}

	return warnings, nil
}

type contextKey string

var relationKey = contextKey("relation")

func addDefinitionWarnings(ctx context.Context, def *corev1.NamespaceDefinition, resolver typesystem.Resolver) ([]*devinterface.DeveloperWarning, error) {
	ts, err := typesystem.NewNamespaceTypeSystem(def, resolver)
	if err != nil {
		return nil, err
	}

	warnings := []*devinterface.DeveloperWarning{}
	for _, rel := range def.Relation {
		ctx = context.WithValue(ctx, relationKey, rel)
		for _, checker := range allChecks.relationCheckers {
			checkerWarning, err := checker(ctx, rel, ts)
			if err != nil {
				return nil, err
			}

			if checkerWarning != nil {
				warnings = append(warnings, checkerWarning)
			}
		}

		if ts.IsPermission(rel.Name) {
			found, err := walkUsersetRewrite(ctx, rel.UsersetRewrite, allChecks, ts)
			if err != nil {
				return nil, err
			}

			warnings = append(warnings, found...)
		}
	}

	return warnings, nil
}

type (
	relationChecker        func(ctx context.Context, relation *corev1.Relation, vts *typesystem.TypeSystem) (*devinterface.DeveloperWarning, error)
	computedUsersetChecker func(ctx context.Context, computedUserset *corev1.ComputedUserset, vts *typesystem.TypeSystem) (*devinterface.DeveloperWarning, error)
	ttuChecker             func(ctx context.Context, ttu *corev1.TupleToUserset, vts *typesystem.TypeSystem) (*devinterface.DeveloperWarning, error)
)

type checkers struct {
	relationCheckers        []relationChecker
	computedUsersetCheckers []computedUsersetChecker
	ttuCheckers             []ttuChecker
}

func walkUsersetRewrite(ctx context.Context, rewrite *corev1.UsersetRewrite, checkers checkers, ts *typesystem.TypeSystem) ([]*devinterface.DeveloperWarning, error) {
	if rewrite == nil {
		return nil, nil
	}

	switch t := (rewrite.RewriteOperation).(type) {
	case *corev1.UsersetRewrite_Union:
		return walkUsersetOperations(ctx, t.Union.Child, checkers, ts)

	case *corev1.UsersetRewrite_Intersection:
		return walkUsersetOperations(ctx, t.Intersection.Child, checkers, ts)

	case *corev1.UsersetRewrite_Exclusion:
		return walkUsersetOperations(ctx, t.Exclusion.Child, checkers, ts)

	default:
		return nil, spiceerrors.MustBugf("unexpected rewrite operation type %T", t)
	}
}

func walkUsersetOperations(ctx context.Context, ops []*corev1.SetOperation_Child, checkers checkers, ts *typesystem.TypeSystem) ([]*devinterface.DeveloperWarning, error) {
	warnings := []*devinterface.DeveloperWarning{}
	for _, op := range ops {
		switch t := op.ChildType.(type) {
		case *corev1.SetOperation_Child_XThis:
			continue

		case *corev1.SetOperation_Child_ComputedUserset:
			for _, checker := range checkers.computedUsersetCheckers {
				checkerWarning, err := checker(ctx, t.ComputedUserset, ts)
				if err != nil {
					return nil, err
				}

				if checkerWarning != nil {
					warnings = append(warnings, checkerWarning)
				}
			}

		case *corev1.SetOperation_Child_UsersetRewrite:
			found, err := walkUsersetRewrite(ctx, t.UsersetRewrite, checkers, ts)
			if err != nil {
				return nil, err
			}

			warnings = append(warnings, found...)

		case *corev1.SetOperation_Child_TupleToUserset:
			for _, checker := range checkers.ttuCheckers {
				checkerWarning, err := checker(ctx, t.TupleToUserset, ts)
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
