package common

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	tuple1 = "docs:1#reader@user:1"
	tuple2 = "docs:2#editor@user:2"
)

var (
	rev1          = revision.NewFromDecimal(decimal.NewFromInt(1))
	rev2          = revision.NewFromDecimal(decimal.NewFromInt(2))
	revOneMillion = revision.NewFromDecimal(decimal.NewFromInt(1_000_000))
)

func revisionFromTransactionID(txID uint64) revision.Decimal {
	return revision.NewFromDecimal(decimal.NewFromInt(int64(txID)))
}

func TestChanges(t *testing.T) {
	type changeEntry struct {
		revision     uint64
		relationship string
		op           core.RelationTupleUpdate_Operation
	}

	testCases := []struct {
		name     string
		script   []changeEntry
		expected []datastore.RevisionChanges
	}{
		{
			"empty",
			[]changeEntry{},
			[]datastore.RevisionChanges{},
		},
		{
			"create",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"delete",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_DELETE},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{del(tuple1)}},
			},
		},
		{
			"in-order touch",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_DELETE},
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"reverse-order touch",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple1, core.RelationTupleUpdate_DELETE},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"create and delete",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple2, core.RelationTupleUpdate_DELETE},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple2)}},
			},
		},
		{
			"multiple creates",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple2, core.RelationTupleUpdate_TOUCH},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple2)}},
			},
		},
		{
			"duplicates",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"create then touch",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{2, tuple1, core.RelationTupleUpdate_DELETE},
				{2, tuple1, core.RelationTupleUpdate_TOUCH},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
				{Revision: rev2, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"big revision gap",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1_000_000, tuple1, core.RelationTupleUpdate_DELETE},
				{1_000_000, tuple1, core.RelationTupleUpdate_TOUCH},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
				{Revision: revOneMillion, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"out of order",
			[]changeEntry{
				{1_000_000, tuple1, core.RelationTupleUpdate_TOUCH},
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{1_000_000, tuple1, core.RelationTupleUpdate_DELETE},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
				{Revision: revOneMillion, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"kitchen sink",
			[]changeEntry{
				{1, tuple1, core.RelationTupleUpdate_TOUCH},
				{2, tuple1, core.RelationTupleUpdate_DELETE},
				{1_000_000, tuple1, core.RelationTupleUpdate_TOUCH},

				{1, tuple2, core.RelationTupleUpdate_DELETE},
				{2, tuple2, core.RelationTupleUpdate_TOUCH},
				{1_000_000, tuple2, core.RelationTupleUpdate_DELETE},
				{1_000_000, tuple2, core.RelationTupleUpdate_TOUCH},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple2)}},
				{Revision: rev2, Changes: []*core.RelationTupleUpdate{touch(tuple2), del(tuple1)}},
				{Revision: revOneMillion, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple2)}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ctx := context.Background()
			ch := NewChanges(revision.DecimalKeyFunc)
			for _, step := range tc.script {
				rel := tuple.MustParse(step.relationship)
				ch.AddChange(ctx, revisionFromTransactionID(step.revision), rel, step.op)
			}

			require.Equal(
				canonicalize(tc.expected),
				canonicalize(ch.AsRevisionChanges(revision.DecimalKeyLessThanFunc)),
			)
		})
	}
}

func TestCanonicalize(t *testing.T) {
	testCases := []struct {
		name            string
		input, expected []datastore.RevisionChanges
	}{
		{
			"empty",
			[]datastore.RevisionChanges{},
			[]datastore.RevisionChanges{},
		},
		{
			"single entries",
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1)}},
			},
		},
		{
			"tuples out of order",
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{del(tuple2), touch(tuple1)}},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple2)}},
			},
		},
		{
			"operations out of order",
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{del(tuple1), touch(tuple1)}},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple1)}},
			},
		},
		{
			"equal entries",
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple1)}},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple1)}},
			},
		},
		{
			"already canonical",
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple2)}},
				{Revision: rev2, Changes: []*core.RelationTupleUpdate{del(tuple1), touch(tuple2)}},
				{Revision: revOneMillion, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple2)}},
			},
			[]datastore.RevisionChanges{
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple2)}},
				{Revision: rev2, Changes: []*core.RelationTupleUpdate{del(tuple1), touch(tuple2)}},
				{Revision: revOneMillion, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple2)}},
			},
		},
		{
			"revisions allowed out of order",
			[]datastore.RevisionChanges{
				{Revision: revOneMillion, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple2)}},
				{Revision: rev2, Changes: []*core.RelationTupleUpdate{del(tuple1), touch(tuple2)}},
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple2)}},
			},
			[]datastore.RevisionChanges{
				{Revision: revOneMillion, Changes: []*core.RelationTupleUpdate{touch(tuple1), touch(tuple2)}},
				{Revision: rev2, Changes: []*core.RelationTupleUpdate{del(tuple1), touch(tuple2)}},
				{Revision: rev1, Changes: []*core.RelationTupleUpdate{touch(tuple1), del(tuple2)}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			require.Equal(tc.expected, canonicalize(tc.input))
		})
	}
}

func touch(relationship string) *core.RelationTupleUpdate {
	return &core.RelationTupleUpdate{
		Operation: core.RelationTupleUpdate_TOUCH,
		Tuple:     tuple.MustParse(relationship),
	}
}

func del(relationship string) *core.RelationTupleUpdate {
	return &core.RelationTupleUpdate{
		Operation: core.RelationTupleUpdate_DELETE,
		Tuple:     tuple.MustParse(relationship),
	}
}

func canonicalize(in []datastore.RevisionChanges) []datastore.RevisionChanges {
	out := make([]datastore.RevisionChanges, 0, len(in))

	for _, rev := range in {
		outChanges := make([]*core.RelationTupleUpdate, 0, len(rev.Changes))

		outChanges = append(outChanges, rev.Changes...)
		sort.Slice(outChanges, func(i, j int) bool {
			// Return if i < j
			left, right := outChanges[i], outChanges[j]
			tupleCompareResult := strings.Compare(tuple.StringWithoutCaveat(left.Tuple), tuple.StringWithoutCaveat(right.Tuple))
			if tupleCompareResult < 0 {
				return true
			}
			if tupleCompareResult > 0 {
				return false
			}

			// Tuples are equal, sort by op
			return left.Operation < right.Operation
		})

		out = append(out, datastore.RevisionChanges{
			Revision: rev.Revision,
			Changes:  outChanges,
		})
	}

	return out
}
