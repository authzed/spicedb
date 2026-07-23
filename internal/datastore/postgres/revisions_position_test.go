//go:build datastore && postgres

package postgres

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestPositionedRevisionEncoding asserts the string round-trip and
// byte-sortability of revisions carrying a commit position.
func TestPositionedRevisionEncoding(t *testing.T) {
	testCases := []struct {
		name             string
		revision         postgresRevision
		wantByteSortable bool
	}{
		{
			name: "a revision with a commit position",
			revision: postgresRevision{
				snapshot:          pgSnapshot{xmin: 1000, xmax: 1005, xipList: []uint64{1002, 1003}},
				optionalTxID:      NewXid8(1004),
				optionalCommitLSN: 0x16CD3F0000028,
			},
			wantByteSortable: true,
		},
		{
			name: "a revision delivered from a different cursor carries the same position",
			revision: postgresRevision{
				snapshot:          pgSnapshot{xmin: 1000, xmax: 1005, xipList: []uint64{1002, 1003}},
				optionalTxID:      NewXid8(1001),
				optionalCommitLSN: 0x16CD3F0000028,
			},
			wantByteSortable: true,
		},
		{
			name:             "a revision without a position keeps the legacy encoding",
			revision:         postgresRevision{snapshot: pgSnapshot{xmin: 5, xmax: 5}},
			wantByteSortable: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantByteSortable, tc.revision.ByteSortable())

			parsed, err := ParseRevisionString(tc.revision.String())
			require.NoError(t, err)

			parsedRev, ok := parsed.(postgresRevision)
			require.True(t, ok)
			require.Equal(t, tc.revision.optionalCommitLSN, parsedRev.optionalCommitLSN)
			require.Equal(t, tc.revision.snapshot, parsedRev.snapshot)
			require.Equal(t, tc.revision.optionalTxID, parsedRev.optionalTxID)
			require.True(t, tc.revision.Equal(parsedRev))
		})
	}

	t.Run("string ordering matches commit position ordering", func(t *testing.T) {
		revisions := []postgresRevision{
			{snapshot: pgSnapshot{xmin: 90, xmax: 90}, optionalCommitLSN: 0xFF000000FF},
			{snapshot: pgSnapshot{xmin: 10, xmax: 10}, optionalCommitLSN: 1},
			{snapshot: pgSnapshot{xmin: 50, xmax: 50}, optionalCommitLSN: 0x1000000000000000},
			{snapshot: pgSnapshot{xmin: 20, xmax: 20}, optionalCommitLSN: 0x20},
			{snapshot: pgSnapshot{xmin: 30, xmax: 30}, optionalCommitLSN: 0x21},
			{snapshot: pgSnapshot{xmin: 40, xmax: 40}, optionalCommitLSN: 0x22},
		}

		asStrings := make([]string, 0, len(revisions))
		for _, rev := range revisions {
			asStrings = append(asStrings, rev.String())
		}
		sort.Strings(asStrings)

		sort.Slice(revisions, func(i, j int) bool {
			return revisions[i].optionalCommitLSN < revisions[j].optionalCommitLSN
		})

		for index, rev := range revisions {
			require.Equal(t, rev.String(), asStrings[index], "byte ordering of revision strings must match position ordering")
		}
	})

	// The same transaction reported by two watch calls is the same token: the
	// position belongs to the transaction, not to the call that delivered it.
	t.Run("identical transactions produce identical tokens", func(t *testing.T) {
		delivered := postgresRevision{
			snapshot:                      pgSnapshot{xmin: 700, xmax: 702, xipList: []uint64{701}},
			optionalTxID:                  NewXid8(700),
			optionalInexactNanosTimestamp: 1_700_000_000_000_000_000,
			optionalCommitLSN:             0x3F00A8,
		}
		redelivered := delivered

		require.Equal(t, delivered.String(), redelivered.String())
		require.True(t, delivered.Equal(redelivered))
	})
}

// TestPositionedRevisionOrdering asserts position-first comparison semantics,
// with snapshot fallback when either side lacks a position.
func TestPositionedRevisionOrdering(t *testing.T) {
	testCases := []struct {
		name        string
		lhs         postgresRevision
		rhs         datastore.Revision
		wantGreater bool
		wantLess    bool
		wantEqual   bool
	}{
		{
			name:        "a later position is greater regardless of snapshots",
			lhs:         postgresRevision{snapshot: pgSnapshot{xmin: 90, xmax: 90}, optionalCommitLSN: 600},
			rhs:         postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}, optionalCommitLSN: 500},
			wantGreater: true,
		},
		{
			name:     "an earlier position is less",
			lhs:      postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}, optionalCommitLSN: 500},
			rhs:      postgresRevision{snapshot: pgSnapshot{xmin: 90, xmax: 90}, optionalCommitLSN: 600},
			wantLess: true,
		},
		{
			name:      "the same position is equal",
			lhs:       postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}, optionalCommitLSN: 500},
			rhs:       postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}, optionalCommitLSN: 500},
			wantEqual: true,
		},
		{
			name:      "snapshot semantics when either side lacks a position",
			lhs:       postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}, optionalCommitLSN: 500},
			rhs:       postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}},
			wantEqual: true,
		},
		{
			name:        "always greater than NoRevision",
			lhs:         postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}, optionalCommitLSN: 500},
			rhs:         datastore.NoRevision,
			wantGreater: true,
		},
		{
			// An overlapping transaction pair: each snapshot knows its own
			// transaction settled and has the other in flight, so the snapshots
			// abstain and the commit positions supply the order.
			name:        "overlapping transactions are ordered by commit position",
			lhs:         postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 102, xipList: []uint64{100}}, optionalCommitLSN: 0x740},
			rhs:         postgresRevision{snapshot: pgSnapshot{xmin: 101, xmax: 101}, optionalCommitLSN: 0x500},
			wantGreater: true,
		},
		{
			// Without positions the same pair is incomparable in every
			// direction, because the snapshot partial order abstains.
			name: "overlapping transactions without positions are incomparable",
			lhs:  postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 102, xipList: []uint64{100}}},
			rhs:  postgresRevision{snapshot: pgSnapshot{xmin: 101, xmax: 101}},
		},
		{
			name:     "an earlier transaction sorts below a later one",
			lhs:      postgresRevision{snapshot: pgSnapshot{xmin: 90, xmax: 90}, optionalCommitLSN: 0x500},
			rhs:      postgresRevision{snapshot: pgSnapshot{xmin: 100, xmax: 100}, optionalCommitLSN: 0x740},
			wantLess: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantGreater, tc.lhs.GreaterThan(tc.rhs))
			require.Equal(t, tc.wantLess, tc.lhs.LessThan(tc.rhs))
			require.Equal(t, tc.wantEqual, tc.lhs.Equal(tc.rhs))
		})
	}
}

// TestDecomposeRevisionChanges asserts that an assembled change is split into
// independent single-item events that all share its revision, which is what the
// EmitImmediatelyStrategy delivers.
func TestDecomposeRevisionChanges(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"origin": "backfill"})
	require.NoError(t, err)

	revision := postgresRevision{snapshot: pgSnapshot{xmin: 10, xmax: 10}, optionalCommitLSN: 0x480}
	atoms := decomposeRevisionChanges(datastore.RevisionChanges{
		Revision: revision,
		RelationshipChanges: []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("document:doc1#viewer@user:alice")),
		},
		ChangedDefinitions: []datastore.SchemaDefinition{&core.NamespaceDefinition{Name: "somenamespace"}},
		DeletedNamespaces:  []string{"deletedns"},
		DeletedCaveats:     []string{"deletedcaveat"},
		Metadatas:          []*structpb.Struct{metadata},
	})

	require.Len(t, atoms, 5)
	require.Len(t, atoms[0].RelationshipChanges, 1)
	require.Equal(t, "somenamespace", atoms[1].ChangedDefinitions[0].GetName())
	require.Equal(t, []string{"deletedns"}, atoms[2].DeletedNamespaces)
	require.Equal(t, []string{"deletedcaveat"}, atoms[3].DeletedCaveats)
	require.Len(t, atoms[4].Metadatas, 1)
	for _, atom := range atoms {
		require.True(t, atom.Revision.Equal(revision))
	}
}

// TestLedgerConnectRejectsInvalidURL asserts that the ledger's replication
// connection reports a malformed connection string instead of panicking.
func TestLedgerConnectRejectsInvalidURL(t *testing.T) {
	pgd := &pgDatastore{dburl: "://not-a-valid-url"}
	_, err := pgd.connectLogicalReplication(t.Context())
	require.Error(t, err)
}
