package development

import (
	"context"
	"fmt"
	"slices"

	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/tuple"
)

// validateCompiledPartials checks the type references inside each partial in
// the compiled schema so that schema-level errors (e.g. an
// `allowedDirectRelation` pointing at an undefined object definition or
// caveat) are reported against the partial's own source position rather than
// being lost when the partial is unused, or being misattributed to the
// consumer that first inlines it.
//
// Only relations that are NOT inlined into any object definition are validated
// here. When a partial is consumed, the consumer's typesystem validation
// already surfaces the same error, and double-emitting would produce
// confusing duplicate diagnostics. The lost UX of "wrong attribution for
// consumed partials" is left to a separate enhancement.
//
// The checks intentionally exclude validations that depend on consumer-
// supplied state (computed userset / TTU operands referring to relations the
// consumer contributes); only `allowedDirectRelation` and caveat references
// are resolved, since those are fully determined by the partial body.
func validateCompiledPartials(ctx context.Context, compiled *compiler.CompiledSchema) []*devinterface.DeveloperError {
	if len(compiled.PartialRelationOrigins) == 0 {
		return nil
	}

	// Build the set of relation pointers that are inlined into any object
	// definition. Those will be validated by the consumer's typesystem pass
	// in loadCompiled and must not be re-reported here.
	consumed := make(map[*core.Relation]struct{})
	for _, nsDef := range compiled.ObjectDefinitions {
		for _, rel := range nsDef.GetRelation() {
			consumed[rel] = struct{}{}
		}
	}

	resolver := schema.ResolverForCompiledSchema(compiled)

	// Synthesize each partial as a namespace definition so error attribution
	// can use the partial's name as the schema path and so self-references
	// inside the partial's body can be resolved without leaking into the
	// real namespace resolver.
	syntheticDefs := make(map[string]*core.NamespaceDefinition, len(compiled.CompiledPartials))
	for name, rels := range compiled.CompiledPartials {
		syntheticDefs[name] = namespace.Namespace(name, rels...)
	}

	// Group relations by origin partial so each relation is validated exactly
	// once against the partial that actually declared it. Sort relation
	// pointers within each group by name for deterministic error ordering.
	type pendingRelation struct {
		rel *core.Relation
	}
	byOrigin := make(map[string][]pendingRelation, len(syntheticDefs))
	for rel, origin := range compiled.PartialRelationOrigins {
		if _, isConsumed := consumed[rel]; isConsumed {
			continue
		}
		byOrigin[origin] = append(byOrigin[origin], pendingRelation{rel: rel})
	}
	if len(byOrigin) == 0 {
		return nil
	}

	originNames := make([]string, 0, len(byOrigin))
	for name := range byOrigin {
		originNames = append(originNames, name)
	}
	slices.Sort(originNames)

	var errors []*devinterface.DeveloperError
	for _, origin := range originNames {
		owningDef := syntheticDefs[origin]
		if owningDef == nil {
			continue
		}
		pending := byOrigin[origin]
		slices.SortFunc(pending, func(a, b pendingRelation) int {
			if a.rel.GetName() == b.rel.GetName() {
				return 0
			}
			if a.rel.GetName() < b.rel.GetName() {
				return -1
			}
			return 1
		})
		for _, p := range pending {
			typeInfo := p.rel.GetTypeInformation()
			if typeInfo == nil {
				continue
			}
			for _, allowed := range typeInfo.GetAllowedDirectRelations() {
				if err := validatePartialAllowedRelation(ctx, resolver, p.rel, allowed); err != nil {
					if devErr := getDevError(err, compiled, owningDef); devErr != nil {
						errors = append(errors, devErr)
					}
				}
			}
		}
	}
	return errors
}

// validatePartialAllowedRelation checks that the object type (and, where
// applicable, the referenced relation on that type) named by an
// allowed-direct-relation in a partial resolves against the real schema.
// Partial names are NOT accepted as subject types: partials are inlined into
// definitions, they are not runtime types of their own. That includes the
// partial referring to itself.
func validatePartialAllowedRelation(
	ctx context.Context,
	resolver *schema.CompiledSchemaResolver,
	rel *core.Relation,
	allowed *core.AllowedRelation,
) error {
	subjectNamespace := allowed.GetNamespace()

	def, _, err := resolver.LookupDefinition(ctx, subjectNamespace)
	if err != nil {
		return schema.NewTypeWithSourceError(
			fmt.Errorf("could not lookup definition `%s` for relation `%s`: %w", subjectNamespace, rel.GetName(), err),
			allowed,
			subjectNamespace,
		)
	}

	// Check that the named relation on the subject namespace exists,
	// mirroring the typesystem's behavior for non-self subjects.
	if allowed.GetPublicWildcard() == nil && allowed.GetRelation() != tuple.Ellipsis {
		if !relationExists(def, allowed.GetRelation()) {
			return schema.NewTypeWithSourceError(
				schema.NewRelationNotFoundErr(subjectNamespace, allowed.GetRelation()),
				allowed,
				allowed.GetRelation(),
			)
		}
	}

	return validatePartialCaveatReference(ctx, resolver, rel, allowed)
}

func validatePartialCaveatReference(
	ctx context.Context,
	resolver *schema.CompiledSchemaResolver,
	rel *core.Relation,
	allowed *core.AllowedRelation,
) error {
	caveatRef := allowed.GetRequiredCaveat()
	if caveatRef == nil {
		return nil
	}
	if _, err := resolver.LookupCaveat(ctx, caveatRef.GetCaveatName()); err != nil {
		return schema.NewTypeWithSourceError(
			fmt.Errorf("could not lookup caveat `%s` for relation `%s`: %w", caveatRef.GetCaveatName(), rel.GetName(), err),
			allowed,
			schema.SourceForAllowedRelation(allowed),
		)
	}
	return nil
}

func relationExists(def *core.NamespaceDefinition, relationName string) bool {
	for _, r := range def.GetRelation() {
		if r.GetName() == relationName {
			return true
		}
	}
	return false
}
