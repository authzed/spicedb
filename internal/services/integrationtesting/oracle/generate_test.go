package oracle_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/types/known/structpb"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/internal/services/integrationtesting/oracle"
	"github.com/authzed/spicedb/internal/services/integrationtesting/testconfigs"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

// objectIDs are the IDs generated relationships draw from, for every type. A few objects
// per type are enough to form chains, cycles and fan-outs.
var objectIDs = []string{"a", "b", "c"}

// TestGeneratedDataAgreesWithOracle generates relationships for the schema of every
// validation file, and requires check to agree with the oracle on each generated dataset.
//
// Run more cases with -rapid.checks=N; a failure prints a validation file to reproduce it.
func TestGeneratedDataAgreesWithOracle(t *testing.T) {
	clingo := oracle.ClingoFromEnv()
	if clingo == nil {
		t.Skip("clingo not found; install it or set SPICEDB_CLINGO")
	}

	fileNames, err := testconfigs.List()
	require.NoError(t, err)

	for _, fileName := range fileNames {
		t.Run(fileName, func(t *testing.T) {
			contents, err := testconfigs.FS.ReadFile(fileName)
			require.NoError(t, err)

			fixture, _ := populate(t, contents)
			slots := slotsFor(fixture)
			if len(slots) == 0 {
				t.Skip("no relations the generator can write")
			}
			dispatcher := consistencytestutil.CreateDispatcherForTesting(t, false)

			var paradoxes, unsupported, depthExceeded int
			rapid.Check(t, func(rt *rapid.T) {
				generated := renderFile(fixture.Schema, drawRelationships(rt, slots))
				populated, revision := populate(t, []byte(generated))
				ctx := populated.ctx

				result, err := oracle.Compute(ctx, clingo, populated.PopulatedValidationFile, time.Now())
				switch {
				case errors.As(err, new(oracle.ErrNoUniqueModel)):
					paradoxes++
					return
				case errors.As(err, new(oracle.ErrUnsupported)):
					unsupported++
					return
				case err != nil:
					rt.Fatalf("oracle: %v\n%s", err, generated)
				}

				problems, depthErrors := compareCheck(ctx, dispatcher, revision, populated.PopulatedValidationFile, result)
				if depthErrors > 0 {
					// Known: check's depth-first traversal cannot answer over cycles in the data.
					depthExceeded++
				}
				if len(problems) > 0 {
					rt.Fatalf("check disagrees with the oracle:\n  %s\n\nvalidation file:\n%s",
						strings.Join(problems, "\n  "), generated)
				}
			})
			if paradoxes+unsupported+depthExceeded > 0 {
				t.Logf("skipped %d paradoxical and %d unsupported datasets; check exceeded its depth on %d",
					paradoxes, unsupported, depthExceeded)
			}
		})
	}
}

type populatedFile struct {
	*validationfile.PopulatedValidationFile
	ctx context.Context
}

// populate loads a validation file into a fresh in-memory datastore.
func populate(t *testing.T, contents []byte) (populatedFile, datastore.Revision) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, time.Second, memdb.DisableGC)
	require.NoError(t, err)
	dl := datalayer.NewDataLayer(ds)
	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(ctx, dl))

	populated, revision, err := validationfile.PopulateFromFilesContents(ctx, dl, caveattypes.Default.TypeSet, map[string][]byte{"file.yaml": contents})
	require.NoError(t, err, "%s", contents)
	return populatedFile{populated, ctx}, revision
}

// slot is a kind of relationship the schema allows: a relation, and one of its allowed
// subject types.
type slot struct {
	resourceType string
	relation     string
	allowed      *core.AllowedRelation
	caveat       *core.CaveatDefinition

	// values holds, for each caveat parameter, the values to draw from. Parameters of
	// types the oracle cannot enumerate are always written.
	values     map[string][]any
	alwaysSet  map[string]bool
	expiration bool
}

var scalarValues = map[string][]any{
	"bool":      {false, true},
	"int":       {int64(0), int64(1), int64(42)},
	"uint":      {uint64(0), uint64(1), uint64(42)},
	"double":    {0.0, 1.5, 42.0},
	"string":    {"", "a", "hello"},
	"timestamp": {"2020-01-01T00:00:00Z", "2023-06-01T00:00:00Z", "2030-01-01T00:00:00Z"},
}

// slotsFor returns every kind of relationship the generator can write for the fixture's
// schema. Caveat values are drawn from a few generic values plus the values the fixture
// itself writes, which tend to sit on the caveat's boundaries.
func slotsFor(fixture populatedFile) []slot {
	caveats := map[string]*core.CaveatDefinition{}
	for _, def := range fixture.CaveatDefinitions {
		caveats[def.Name] = def
	}
	written := map[string]map[string][]any{}
	for _, rel := range fixture.Relationships {
		if rel.OptionalCaveat == nil {
			continue
		}
		name := rel.OptionalCaveat.CaveatName
		if written[name] == nil {
			written[name] = map[string][]any{}
		}
		for param, v := range rel.OptionalCaveat.GetContext().AsMap() {
			written[name][param] = append(written[name][param], v)
		}
	}

	var slots []slot
	for _, def := range fixture.NamespaceDefinitions {
		for _, rel := range def.Relation {
			if rel.UsersetRewrite != nil || rel.TypeInformation == nil {
				continue
			}
		allowed:
			for _, allowed := range rel.TypeInformation.AllowedDirectRelations {
				s := slot{
					resourceType: def.Name,
					relation:     rel.Name,
					allowed:      allowed,
					values:       map[string][]any{},
					alwaysSet:    map[string]bool{},
					expiration:   allowed.RequiredExpiration != nil,
				}
				if allowed.RequiredCaveat != nil {
					s.caveat = caveats[allowed.RequiredCaveat.CaveatName]
					for param, typ := range s.caveat.ParameterTypes {
						fromFixture := written[s.caveat.Name][param]
						generic, scalar := scalarValues[typ.TypeName]
						switch {
						case scalar:
							s.values[param] = append(slices.Clone(generic), fromFixture...)
						case len(fromFixture) > 0 && typ.TypeName != "ipaddress":
							s.values[param], s.alwaysSet[param] = fromFixture, true
						default:
							// Nothing the oracle can evaluate: leave this kind of relationship out.
							continue allowed
						}
					}
				}
				slots = append(slots, s)
			}
		}
	}
	return slots
}

// drawRelationships draws up to a dozen distinct relationships.
func drawRelationships(rt *rapid.T, slots []slot) []tuple.Relationship {
	count := rapid.IntRange(0, 12).Draw(rt, "count")
	seen := map[string]bool{}
	var rels []tuple.Relationship
	for i := range count {
		s := rapid.SampledFrom(slots).Draw(rt, fmt.Sprintf("slot%d", i))

		subject := tuple.ONR(s.allowed.Namespace, rapid.SampledFrom(objectIDs).Draw(rt, fmt.Sprintf("subject%d", i)), tuple.Ellipsis)
		if s.allowed.GetPublicWildcard() != nil {
			subject.ObjectID = tuple.PublicWildcard
		} else if r := s.allowed.GetRelation(); r != "" {
			subject.Relation = r
		}
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR(s.resourceType, rapid.SampledFrom(objectIDs).Draw(rt, fmt.Sprintf("resource%d", i)), s.relation),
				Subject:  subject,
			},
		}

		// A relationship can be written only once, whatever its caveat.
		if key := tuple.MustString(rel); seen[key] {
			continue
		} else {
			seen[key] = true
		}

		if s.caveat != nil {
			context := map[string]any{}
			for _, param := range slices.Sorted(maps.Keys(s.values)) {
				label := fmt.Sprintf("rel%d.%s", i, param)
				if s.alwaysSet[param] || rapid.Bool().Draw(rt, label+".set") {
					context[param] = rapid.SampledFrom(s.values[param]).Draw(rt, label)
				}
			}
			structContext, err := structpb.NewStruct(context)
			if err != nil {
				rt.Fatalf("caveat context: %v", err)
			}
			rel.OptionalCaveat = &core.ContextualizedCaveat{CaveatName: s.caveat.Name, Context: structContext}
		}
		if s.expiration {
			// Clearly expired, or clearly not, so the outcome never depends on timing.
			expiration := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			if rapid.Bool().Draw(rt, fmt.Sprintf("rel%d.live", i)) {
				expiration = time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			rel.OptionalExpiration = &expiration
		}
		rels = append(rels, rel)
	}
	return rels
}

// renderFile returns a validation file with the schema and relationships.
func renderFile(schema string, rels []tuple.Relationship) string {
	lines := make([]string, 0, len(rels))
	for _, rel := range rels {
		lines = append(lines, tuple.MustString(rel))
	}
	out, err := yaml.Marshal(map[string]string{
		"schema":        schema,
		"relationships": strings.Join(lines, "\n"),
	})
	if err != nil {
		panic(err)
	}
	return string(out)
}

// compareCheck runs check for every resource and subject, and returns every disagreement
// with the oracle. Wherever a caveat is involved, it also compares every world. Checks that
// exceed their maximum depth are counted rather than reported.
func compareCheck(
	ctx context.Context,
	dispatcher dispatch.Check,
	revision datastore.Revision,
	populated *validationfile.PopulatedValidationFile,
	result *oracle.Result,
) (problems []string, depthErrors int) {
	subjects := map[string]tuple.ObjectAndRelation{}
	for _, rel := range populated.Relationships {
		if rel.Subject.ObjectID != tuple.PublicWildcard {
			subjects[tuple.StringONR(rel.Subject)] = rel.Subject
		}
	}
	relations := map[string][]string{}
	for _, def := range populated.NamespaceDefinitions {
		for _, rel := range def.Relation {
			relations[def.Name] = append(relations[def.Name], rel.Name)
		}
	}

	for _, object := range result.Objects {
		for _, relation := range relations[object.ObjectType] {
			resource := tuple.ONR(object.ObjectType, object.ObjectID, relation)
			for _, subject := range subjects {
				permString := tuple.MustString(tuple.Relationship{
					RelationshipReference: tuple.RelationshipReference{Resource: resource, Subject: subject},
				})

				fromCheck, err := check(ctx, dispatcher, revision, resource, subject, nil)
				if isMaxDepth(err) {
					depthErrors++
					continue
				}
				if err != nil {
					problems = append(problems, fmt.Sprintf("%s: check failed: %v", permString, err))
					continue
				}
				fromOracle := result.Membership(resource, subject)
				switch {
				case sameMembership(fromCheck, fromOracle):
				case fromCheck == dispatchv1.ResourceCheckResult_CAVEATED_MEMBER:
					// Check reports a caveat whose outcome does not depend on the context.
				default:
					problems = append(problems, fmt.Sprintf("%s: oracle says %s, check says %s", permString, fromOracle, fromCheck))
				}

				if fromCheck != dispatchv1.ResourceCheckResult_CAVEATED_MEMBER && fromOracle != oracle.Caveated {
					continue
				}
				for index, world := range result.Worlds {
					inWorld, err := check(ctx, dispatcher, revision, resource, subject, world)
					if isMaxDepth(err) {
						depthErrors++
						break
					}
					if err != nil {
						problems = append(problems, fmt.Sprintf("%s with %v: check failed: %v", permString, world, err))
						break
					}
					want := result.HoldsIn(resource, subject, index)
					if got := inWorld == dispatchv1.ResourceCheckResult_MEMBER; got != want || inWorld == dispatchv1.ResourceCheckResult_CAVEATED_MEMBER {
						problems = append(problems, fmt.Sprintf("%s with %v: oracle says %v, check says %s", permString, world, want, inWorld))
						break
					}
				}
			}
		}
	}
	slices.Sort(problems)
	return problems, depthErrors
}

func isMaxDepth(err error) bool {
	return err != nil && strings.Contains(err.Error(), "max depth exceeded")
}

func check(
	ctx context.Context,
	dispatcher dispatch.Check,
	revision datastore.Revision,
	resource, subject tuple.ObjectAndRelation,
	caveatContext map[string]any,
) (dispatchv1.ResourceCheckResult_Membership, error) {
	cr, _, err := computed.ComputeCheck(ctx, dispatcher, caveattypes.Default.TypeSet,
		computed.CheckParameters{
			ResourceType:  resource.RelationReference(),
			Subject:       subject,
			CaveatContext: caveatContext,
			AtRevision:    revision,
			MaximumDepth:  50,
			SchemaHash:    datalayer.NoSchemaHashForTesting,
		},
		resource.ObjectID,
		100,
	)
	if err != nil {
		return dispatchv1.ResourceCheckResult_UNKNOWN, err
	}
	return cr.Membership, nil
}
