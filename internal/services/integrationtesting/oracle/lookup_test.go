package oracle_test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/internal/services/integrationtesting/oracle"
	"github.com/authzed/spicedb/internal/services/integrationtesting/testconfigs"
	"github.com/authzed/spicedb/internal/testserver"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

// absent is the permissionship of a result a lookup did not return.
const absent = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_UNSPECIFIED

// TestOracleAgreesWithLookups compares the oracle with LookupResources and LookupSubjects,
// for every validation file. Each lookup's results must be the oracle's answers that match
// the lookup: for LookupResources, the resources of the type that have the permission for
// the subject; for LookupSubjects, the subjects of the type that have the permission on the
// resource.
func TestOracleAgreesWithLookups(t *testing.T) {
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
			compareWithLookups(t, clingo, contents)
		})
	}

	t.Run("self.yaml.skip", func(t *testing.T) {
		contents, err := os.ReadFile("../testconfigs/self.yaml.skip")
		require.NoError(t, err)
		compareWithLookups(t, clingo, contents)
	})
}

// lookupFixture holds what every lookup comparison for a validation file needs.
type lookupFixture struct {
	t        *testing.T
	tester   consistencytestutil.ServiceTester
	revision datastore.Revision
	result   *oracle.Result

	// queried are the oracle's resources by type: every definition for every object ID.
	queried map[string][]tuple.ObjectAndRelation

	// concrete are the IDs written in relationships, by type: the objects wildcards cover.
	concrete map[string]*mapz.Set[string]

	imprecise int
}

func compareWithLookups(t *testing.T, clingo oracle.Clingo, contents []byte) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, time.Second, memdb.DisableGC)
	require.NoError(t, err)
	dl := datalayer.NewDataLayer(ds)
	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(ctx, dl))

	populated, revision, err := validationfile.PopulateFromFilesContents(ctx, dl, caveattypes.Default.TypeSet, map[string][]byte{"file.yaml": contents})
	require.NoError(t, err)

	result, err := oracle.Compute(ctx, clingo, populated, time.Now(), oracle.WithLogger(t))
	if _, ok := errors.AsType[oracle.ErrUnsupported](err); ok {
		t.Skip(err.Error())
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			dir := saveArtifacts(t, map[string][]byte{"program.lp": []byte(result.Program), "output.json": result.Output})
			t.Logf("oracle program and output saved to %s", dir)
		}
	})

	f := &lookupFixture{
		t:        t,
		tester:   consistencytestutil.NewServiceTester(testserver.TestClusterWithDispatch(t, 1, ds)[0]),
		revision: revision,
		result:   result,
		queried:  map[string][]tuple.ObjectAndRelation{},
		concrete: map[string]*mapz.Set[string]{},
	}
	for _, object := range result.Objects {
		f.queried[object.ObjectType] = append(f.queried[object.ObjectType], object)
	}

	// The subjects and subject types the lookups ask about are the ones written in
	// relationships, as in the consistency suite.
	subjects := mapz.NewSet[tuple.ObjectAndRelation]()
	subjectTypes := mapz.NewSet[tuple.RelationReference]()
	for _, rel := range populated.Relationships {
		f.addConcrete(rel.Resource)
		subjectTypes.Add(rel.Subject.RelationReference())
		if rel.Subject.ObjectID != tuple.PublicWildcard {
			f.addConcrete(rel.Subject)
			subjects.Add(rel.Subject)
		}
	}

	for _, def := range populated.NamespaceDefinitions {
		for _, rel := range def.Relation {
			resourceType := tuple.RR(def.Name, rel.Name)
			for _, subject := range subjects.AsSlice() {
				f.lookupResources(resourceType, subject)
			}
			for _, resource := range f.queried[def.Name] {
				resource.Relation = rel.Name
				for _, subjectType := range subjectTypes.AsSlice() {
					f.lookupSubjects(resource, subjectType)
				}
			}
		}
	}
	t.Logf("%d imprecise results, %d worlds", f.imprecise, len(result.Worlds))
}

func (f *lookupFixture) addConcrete(onr tuple.ObjectAndRelation) {
	ids, ok := f.concrete[onr.ObjectType]
	if !ok {
		ids = mapz.NewSet[string]()
		f.concrete[onr.ObjectType] = ids
	}
	ids.Add(onr.ObjectID)
}

// lookupResources compares LookupResources for the subject with the oracle's answers for
// every resource of the type.
func (f *lookupFixture) lookupResources(resourceType tuple.RelationReference, subject tuple.ObjectAndRelation) {
	lookup := func(world oracle.World) map[string]v1.LookupPermissionship {
		found, _, err := f.tester.LookupResources(f.t.Context(), resourceType, subject, f.revision, nil, 0, world)
		require.NoError(f.t, err)
		got := map[string]v1.LookupPermissionship{}
		for _, resp := range found {
			got[resp.ResourceObjectId] = merge(got[resp.ResourceObjectId], resp.Permissionship)
		}
		return got
	}

	candidates := map[string]tuple.ObjectAndRelation{}
	for _, resource := range f.queried[resourceType.ObjectType] {
		resource.Relation = resourceType.Relation
		candidates[resource.ObjectID] = resource
	}
	f.compare(
		fmt.Sprintf("LookupResources(%s, %s)", tuple.StringRR(resourceType), tuple.StringONR(subject)),
		lookup,
		candidates,
		func(resource tuple.ObjectAndRelation) (tuple.ObjectAndRelation, tuple.ObjectAndRelation) {
			return resource, subject
		},
	)
}

// lookupSubjects compares LookupSubjects on the resource with the oracle's answers for
// every subject of the type. A wildcard result covers the concrete objects of its type,
// less the subjects it excludes, as the oracle's wildcards do.
func (f *lookupFixture) lookupSubjects(resource tuple.ObjectAndRelation, subjectType tuple.RelationReference) {
	lookup := func(world oracle.World) map[string]v1.LookupPermissionship {
		found, err := f.tester.LookupSubjects(f.t.Context(), resource, subjectType, f.revision, world)
		require.NoError(f.t, err)
		got := map[string]v1.LookupPermissionship{}
		for id, resp := range found {
			if id != tuple.PublicWildcard {
				got[id] = merge(got[id], resp.Subject.Permissionship)
				continue
			}

			excluded := map[string]v1.LookupPermissionship{}
			for _, ex := range resp.ExcludedSubjects {
				excluded[ex.SubjectObjectId] = ex.Permissionship
			}
			for _, concreteID := range f.concreteIDs(subjectType.ObjectType) {
				permissionship := resp.Subject.Permissionship
				switch excluded[concreteID] {
				case v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION:
					continue // excluded outright
				case v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
					permissionship = v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION
				}
				got[concreteID] = merge(got[concreteID], permissionship)
			}
		}
		return got
	}

	candidates := map[string]tuple.ObjectAndRelation{}
	for _, subject := range f.queried[subjectType.ObjectType] {
		subject.Relation = subjectType.Relation
		candidates[subject.ObjectID] = subject
	}
	f.compare(
		fmt.Sprintf("LookupSubjects(%s, %s)", tuple.StringONR(resource), tuple.StringRR(subjectType)),
		lookup,
		candidates,
		func(subject tuple.ObjectAndRelation) (tuple.ObjectAndRelation, tuple.ObjectAndRelation) {
			return resource, subject
		},
	)
}

func (f *lookupFixture) concreteIDs(objectType string) []string {
	ids, ok := f.concrete[objectType]
	if !ok {
		return nil
	}
	return ids.AsSlice()
}

// compare checks a lookup's results against the oracle, for every candidate ID.
//
// Without context, a lookup's permissionship must match the oracle's membership over all
// worlds, except that a lookup may report a result as conditional when its outcome does not
// actually depend on the context. Wherever a caveat is involved, the lookup is rerun with
// the context of each world, and must then agree with the oracle exactly.
func (f *lookupFixture) compare(
	desc string,
	lookup func(world oracle.World) map[string]v1.LookupPermissionship,
	candidates map[string]tuple.ObjectAndRelation,
	pair func(candidate tuple.ObjectAndRelation) (resource, subject tuple.ObjectAndRelation),
) {
	t := f.t
	got := lookup(nil)

	for id := range got {
		if _, ok := candidates[id]; !ok {
			t.Errorf("%s: returned %s, which the oracle has no answer for", desc, id)
		}
	}

	var inEveryWorld []string
	for id, candidate := range candidates {
		resource, subject := pair(candidate)
		fromOracle := f.result.Membership(resource, subject)
		fromLookup := got[id]

		switch {
		case sameLookupMembership(fromLookup, fromOracle):
		case fromLookup == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
			f.imprecise++
			t.Logf("imprecise: %s: %s is %s, but the lookup reports it as conditional", desc, id, fromOracle)
		default:
			t.Errorf("%s: %s: oracle says %s, lookup says %s", desc, id, fromOracle, fromLookup)
		}

		if fromLookup == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION || fromOracle == oracle.Caveated {
			inEveryWorld = append(inEveryWorld, id)
		}
	}
	if len(inEveryWorld) == 0 {
		return
	}

	for index, world := range f.result.Worlds {
		gotInWorld := lookup(world)
		for _, id := range inEveryWorld {
			resource, subject := pair(candidates[id])
			fromOracle := f.result.HoldsIn(resource, subject, index)
			switch gotInWorld[id] {
			case v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
				t.Errorf("%s with %v: %s is conditional in a complete world", desc, world, id)
			case v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION:
				if !fromOracle {
					t.Errorf("%s with %v: %s: oracle says false, lookup says true", desc, world, id)
				}
			default:
				if fromOracle {
					t.Errorf("%s with %v: %s: oracle says true, lookup says false", desc, world, id)
				}
			}
		}
	}
}

func sameLookupMembership(fromLookup v1.LookupPermissionship, fromOracle oracle.Membership) bool {
	switch fromLookup {
	case v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION:
		return fromOracle == oracle.Member
	case v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION:
		return fromOracle == oracle.Caveated
	default:
		return fromOracle == oracle.NotMember
	}
}

// merge combines two results for the same ID: a definite result outranks a conditional one.
func merge(a, b v1.LookupPermissionship) v1.LookupPermissionship {
	if a == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION || b == absent {
		return a
	}
	if b == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION || a == absent {
		return b
	}
	return a
}
