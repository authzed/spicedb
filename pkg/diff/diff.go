package diff

import (
	"github.com/authzed/spicedb/pkg/diff/caveats"
	"github.com/authzed/spicedb/pkg/diff/namespace"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

// DiffableSchema is a schema that can be diffed.
type DiffableSchema struct {
	// ObjectDefinitions holds the object definitions in the schema.
	ObjectDefinitions []*core.NamespaceDefinition

	// CaveatDefinitions holds the caveat definitions in the schema.
	CaveatDefinitions []*core.CaveatDefinition
}

func (ds *DiffableSchema) GetNamespace(namespaceName string) (*core.NamespaceDefinition, bool) {
	for _, ns := range ds.ObjectDefinitions {
		if ns.Name == namespaceName {
			return ns, true
		}
	}

	return nil, false
}

func (ds *DiffableSchema) GetRelation(nsName string, relationName string) (*core.Relation, bool) {
	ns, ok := ds.GetNamespace(nsName)
	if !ok {
		return nil, false
	}

	for _, relation := range ns.Relation {
		if relation.Name == relationName {
			return relation, true
		}
	}

	return nil, false
}

func (ds *DiffableSchema) GetCaveat(caveatName string) (*core.CaveatDefinition, bool) {
	for _, caveat := range ds.CaveatDefinitions {
		if caveat.Name == caveatName {
			return caveat, true
		}
	}

	return nil, false
}

// NewDiffableSchemaFromCompiledSchema creates a new DiffableSchema from a CompiledSchema.
func NewDiffableSchemaFromCompiledSchema(compiled *compiler.CompiledSchema) DiffableSchema {
	return DiffableSchema{
		ObjectDefinitions: compiled.ObjectDefinitions,
		CaveatDefinitions: compiled.CaveatDefinitions,
	}
}

// SchemaDiff holds the diff between two schemas.
type SchemaDiff struct {
	// AddedNamespaces are the namespaces that were added.
	AddedNamespaces []string

	// RemovedNamespaces are the namespaces that were removed.
	RemovedNamespaces []string

	// AddedCaveats are the caveats that were added.
	AddedCaveats []string

	// RemovedCaveats are the caveats that were removed.
	RemovedCaveats []string

	// ChangedNamespaces are the namespaces that were changed.
	ChangedNamespaces map[string]namespace.Diff

	// ChangedCaveats are the caveats that were changed.
	ChangedCaveats map[string]caveats.Diff
}

// DiffSchemas compares two schemas and returns the diff.
func DiffSchemas(existing DiffableSchema, comparison DiffableSchema) (*SchemaDiff, error) {
	existingNamespacesByName := make(map[string]*core.NamespaceDefinition, len(existing.ObjectDefinitions))
	existingNamespaceNames := mapz.NewSet[string]()
	for _, nsDef := range existing.ObjectDefinitions {
		existingNamespacesByName[nsDef.Name] = nsDef
		existingNamespaceNames.Add(nsDef.Name)
	}

	existingCaveatsByName := make(map[string]*core.CaveatDefinition, len(existing.CaveatDefinitions))
	existingCaveatsByNames := mapz.NewSet[string]()
	for _, caveatDef := range existing.CaveatDefinitions {
		existingCaveatsByName[caveatDef.Name] = caveatDef
		existingCaveatsByNames.Add(caveatDef.Name)
	}

	comparisonNamespacesByName := make(map[string]*core.NamespaceDefinition, len(comparison.ObjectDefinitions))
	comparisonNamespaceNames := mapz.NewSet[string]()
	for _, nsDef := range comparison.ObjectDefinitions {
		comparisonNamespacesByName[nsDef.Name] = nsDef
		comparisonNamespaceNames.Add(nsDef.Name)
	}

	comparisonCaveatsByName := make(map[string]*core.CaveatDefinition, len(comparison.CaveatDefinitions))
	comparisonCaveatsByNames := mapz.NewSet[string]()
	for _, caveatDef := range comparison.CaveatDefinitions {
		comparisonCaveatsByName[caveatDef.Name] = caveatDef
		comparisonCaveatsByNames.Add(caveatDef.Name)
	}

	changedNamespaces := make(map[string]namespace.Diff, 0)
	commonNamespaceNames := existingNamespaceNames.Intersect(comparisonNamespaceNames)
	if err := commonNamespaceNames.ForEach(func(name string) error {
		existingNamespace := existingNamespacesByName[name]
		comparisonNamespace := comparisonNamespacesByName[name]

		diff, err := namespace.DiffNamespaces(existingNamespace, comparisonNamespace)
		if err != nil {
			return err
		}

		if len(diff.Deltas()) > 0 {
			changedNamespaces[name] = *diff
		}

		return nil
	}); err != nil {
		return nil, err
	}

	commonCaveatNames := existingCaveatsByNames.Intersect(comparisonCaveatsByNames)
	changedCaveats := make(map[string]caveats.Diff, 0)
	if err := commonCaveatNames.ForEach(func(name string) error {
		existingCaveat := existingCaveatsByName[name]
		comparisonCaveat := comparisonCaveatsByName[name]

		diff, err := caveats.DiffCaveats(existingCaveat, comparisonCaveat)
		if err != nil {
			return err
		}

		if len(diff.Deltas()) > 0 {
			changedCaveats[name] = *diff
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if len(changedNamespaces) == 0 {
		changedNamespaces = nil
	}
	if len(changedCaveats) == 0 {
		changedCaveats = nil
	}

	return &SchemaDiff{
		AddedNamespaces:   comparisonNamespaceNames.Subtract(existingNamespaceNames).AsSlice(),
		RemovedNamespaces: existingNamespaceNames.Subtract(comparisonNamespaceNames).AsSlice(),
		AddedCaveats:      comparisonCaveatsByNames.Subtract(existingCaveatsByNames).AsSlice(),
		RemovedCaveats:    existingCaveatsByNames.Subtract(comparisonCaveatsByNames).AsSlice(),
		ChangedNamespaces: changedNamespaces,
		ChangedCaveats:    changedCaveats,
	}, nil
}
