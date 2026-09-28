package oracle_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

// TestOracleAgreesWithCheck compares the oracle with the accessibility set, which is built
// from SpiceDB's own check, for every validation file.
func TestOracleAgreesWithCheck(t *testing.T) {
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
			compareWithCheck(t, clingo, contents)
		})
	}

	t.Run("self.yaml.skip", func(t *testing.T) {
		contents, err := os.ReadFile("../testconfigs/self.yaml.skip")
		require.NoError(t, err)
		compareWithCheck(t, clingo, contents)
	})
}

func compareWithCheck(t *testing.T, clingo oracle.Clingo, contents []byte) {
	ts := caveattypes.Default.TypeSet

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, time.Second, memdb.DisableGC)
	require.NoError(t, err)
	dl := datalayer.NewDataLayer(ds)
	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(ctx, dl))

	populated, revision, err := validationfile.PopulateFromFilesContents(ctx, dl, ts, map[string][]byte{"file.yaml": contents})
	require.NoError(t, err)

	result, err := oracle.Compute(ctx, clingo, populated, time.Now())
	var unsupported oracle.ErrUnsupported
	if errors.As(err, &unsupported) {
		t.Skip(unsupported.Error())
	}
	var noUnique oracle.ErrNoUniqueModel
	if errors.As(err, &noUnique) {
		t.Fatalf("%v\n%s", err, noUnique.Program)
	}
	require.NoError(t, err)

	accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, ctx, populated, ds)
	dispatcher := consistencytestutil.CreateDispatcherForTesting(t, false)

	var agreed, imprecise int
	for permString, fromCheck := range accessibilitySet.PermissionshipByRelationship {
		parsed := tuple.MustParse(permString)
		fromOracle := result.Membership(parsed.Resource, parsed.Subject)

		switch {
		case sameMembership(fromCheck, fromOracle):
			agreed++

		case fromCheck == dispatchv1.ResourceCheckResult_CAVEATED_MEMBER:
			// Check reports a caveat whose outcome does not actually depend on the context.
			imprecise++
			t.Logf("imprecise: %s is %s, but check reports it as caveated", permString, fromOracle)

		default:
			t.Errorf("%s: oracle says %s, check says %s", permString, fromOracle, fromCheck)
		}

		// Wherever a caveat is involved, check must agree with the oracle in every world.
		if fromCheck != dispatchv1.ResourceCheckResult_CAVEATED_MEMBER && fromOracle != oracle.Caveated {
			continue
		}
		for index, world := range result.Worlds {
			inWorld := checkInWorld(ctx, t, dispatcher, revision, parsed.Resource, parsed.Subject, world)
			if inWorld != result.HoldsIn(parsed.Resource, parsed.Subject, index) {
				t.Errorf("%s with %v: oracle says %v, check says %v", permString, world,
					result.HoldsIn(parsed.Resource, parsed.Subject, index), inWorld)
			}
		}
	}
	t.Logf("%d agreed, %d imprecise, %d worlds", agreed, imprecise, len(result.Worlds))
}

func sameMembership(fromCheck dispatchv1.ResourceCheckResult_Membership, fromOracle oracle.Membership) bool {
	switch fromCheck {
	case dispatchv1.ResourceCheckResult_MEMBER:
		return fromOracle == oracle.Member
	case dispatchv1.ResourceCheckResult_CAVEATED_MEMBER:
		return fromOracle == oracle.Caveated
	default:
		return fromOracle == oracle.NotMember
	}
}

func checkInWorld(
	ctx context.Context,
	t *testing.T,
	dispatcher dispatch.Check,
	revision datastore.Revision,
	resource, subject tuple.ObjectAndRelation,
	world oracle.World,
) bool {
	cr, _, err := computed.ComputeCheck(ctx, dispatcher, caveattypes.Default.TypeSet,
		computed.CheckParameters{
			ResourceType:  resource.RelationReference(),
			Subject:       subject,
			CaveatContext: world,
			AtRevision:    revision,
			MaximumDepth:  50,
			SchemaHash:    datalayer.NoSchemaHashForTesting,
		},
		resource.ObjectID,
		100,
	)
	require.NoError(t, err)
	require.NotEqual(t, dispatchv1.ResourceCheckResult_CAVEATED_MEMBER, cr.Membership, "check is caveated in a complete world")
	return cr.Membership == dispatchv1.ResourceCheckResult_MEMBER
}
