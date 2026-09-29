package checkbaseline

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/authzed/spicedb/internal/services/integrationtesting/testconfigs"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

func fixtureDataset(id string, fsys fs.FS, name string, extra []Case) Dataset {
	return Dataset{ID: id, Family: "fixture", Source: name, Setup: func(ctx context.Context, ds datastore.Datastore) ([]Case, error) {
		loaded, _, err := validationfile.PopulateFromFS(ctx, datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, fsys, name)
		if err != nil {
			return nil, err
		}
		if _, err := datalayer.WriteStoredSchemaForTest(ctx, ds, loaded.Schema); err != nil {
			return nil, err
		}
		var cases []Case
		for _, file := range loaded.ParsedFiles {
			for _, entry := range []struct {
				assertions []blocks.Assertion
				outcome    Outcome
			}{{file.Assertions.AssertTrue, Allow}, {file.Assertions.AssertFalse, Deny}, {file.Assertions.AssertCaveated, Conditional}} {
				for i, a := range entry.assertions {
					r := a.Relationship
					cases = append(cases, Case{ID: fmt.Sprintf("%s/%d", entry.outcome, i), Query: bm.CheckQuery{ResourceType: r.Resource.ObjectType, ResourceID: r.Resource.ObjectID, Permission: r.Resource.Relation, SubjectType: r.Subject.ObjectType, SubjectID: r.Subject.ObjectID, SubjectRelation: r.Subject.Relation}, Context: a.CaveatContext, Expected: Decision{Outcome: entry.outcome}, ClassicDepth: 200, QPDepth: 50})
				}
			}
		}
		return append(cases, extra...), nil
	}}
}
func FixtureDatasets(root string) ([]Dataset, error) {
	names, err := testconfigs.List()
	if err != nil {
		return nil, err
	}
	var out []Dataset
	for _, name := range names {
		out = append(out, fixtureDataset("consistency/"+name, testconfigs.FS, name, nil))
	}
	steel := os.DirFS(filepath.Join(root, "internal/services/steelthreadtesting/testdata"))
	for _, entry := range []struct {
		name  string
		cases []Case
	}{
		{"basic-document.yaml", []Case{check("public-allow", "document:publicdoc", "view", "outsider", Allow), check("public-banned", "document:publicdoc", "view", "user-0", Deny), check("direct-hit", "document:somedoc", "view", "user-6", Allow)}},
		{"document-with-intersect.yaml", []Case{check("hit", "document:somedoc", "view", "c-0-user-6", Allow), check("miss", "document:somedoc", "view", "outsider", Deny)}},
		{"document-with-intersect-arrow.yaml", []Case{check("hit", "document:somedoc", "view", "user-6", Allow), check("miss", "document:somedoc", "view", "outsider", Deny)}},
		{"document-with-traits.yaml", []Case{check("expired", "document:firstdoc", "view", "tom", Deny), check("unexpired", "document:firstdoc", "view", "fred", Allow), check("caveat-false", "document:seconddoc", "view", "tom", Deny), check("caveat-true", "document:seconddoc", "view", "fred", Allow)}},
	} {
		if entry.name == "document-with-traits.yaml" {
			for i := range entry.cases {
				if entry.cases[i].Query.ResourceID == "seconddoc" {
					entry.cases[i].Context = map[string]any{"somecondition": float64(42)}
				}
			}
			entry.cases = append(entry.cases, check("missing-context", "document:seconddoc", "view", "fred", Conditional))
		}
		out = append(out, fixtureDataset("steelthread/"+entry.name, steel, entry.name, entry.cases))
	}
	return out, nil
}
func Catalog(root string) ([]Dataset, error) {
	fixtures, err := FixtureDatasets(root)
	if err != nil {
		return nil, err
	}
	out := append(RegistryDatasets(), GeneratedDatasets(DefaultScales())...)
	out = append(out, fixtures...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
