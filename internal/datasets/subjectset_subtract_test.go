package datasets

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/testutil"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestBatchedSubtractMatchesSequential(t *testing.T) {
	expressions := []*core.CaveatExpression{nil, caveatexpr("a"), caveatexpr("b")}
	for wi, wexpr := range expressions {
		for ei, eexpr := range expressions {
			for ri, rexpr := range expressions {
				for _, count := range []int{0, 1, 2, 10} {
					t.Run(fmt.Sprintf("%d/%d/%d/%d", wi, ei, ri, count), func(t *testing.T) {
						initial := NewSubjectSet()
						initial.MustAdd(cwc(wexpr, csub("u0", eexpr), csub("retained", eexpr)))
						initial.MustAdd(csub("u1", caveatexpr("c")))
						initial.MustAdd(sub("unrelated"))
						removing := NewSubjectSet()
						for i := range count {
							removing.MustAdd(csub(fmt.Sprintf("u%d", i), rexpr))
						}
						original := cloneSubjects(initial.AsSlice())
						reference := initial.Clone()
						actual := initial.Clone()
						beforeRemoving := cloneSubjects(removing.AsSlice())
						for _, subject := range removing.AsSlice() {
							reference.Subtract(subject)
						}
						actual.SubtractAll(removing)
						testutil.RequireEquivalentSets(t, reference.AsSlice(), actual.AsSlice())
						testutil.RequireEquivalentSets(t, original, initial.AsSlice())
						testutil.RequireEquivalentSets(t, beforeRemoving, removing.AsSlice())
					})
				}
			}
		}
	}
	for _, wildcard := range []*v1.FoundSubject{wc(), cwc(caveatexpr("a"), csub("u0", caveatexpr("b")))} {
		initial := NewSubjectSet()
		initial.MustAdd(wildcard)
		initial.MustAdd(csub("u1", caveatexpr("c")))
		reference := initial.Clone()
		actual := initial.Clone()
		for _, subject := range reference.AsSlice() {
			reference.Subtract(subject)
		}
		actual.SubtractAll(actual)
		testutil.RequireEquivalentSets(t, reference.AsSlice(), actual.AsSlice())
	}
}

func cloneSubjects(subjects []*v1.FoundSubject) []*v1.FoundSubject {
	cloned := make([]*v1.FoundSubject, len(subjects))
	for i, subject := range subjects {
		cloned[i] = proto.Clone(subject).(*v1.FoundSubject)
	}
	return cloned
}

func BenchmarkSubjectSetSubtractAll(b *testing.B) {
	for _, tc := range []struct {
		name                                string
		existingCount, removalCount, offset int
	}{
		{"wildcard/5000", 0, 5000, 0},
		{"existing5000/disjoint10", 5000, 10, 5000},
		{"existing5000/overlap10", 5000, 10, 0},
		{"existing5000/overlap5000", 5000, 5000, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			excluded := make([]string, tc.existingCount)
			for i := range excluded {
				excluded[i] = fmt.Sprintf("u%d", i)
			}
			wildcard := wc(excluded...)
			removing := NewSubjectSet()
			for i := range tc.removalCount {
				removing.MustAdd(sub(fmt.Sprintf("u%d", tc.offset+i)))
			}
			b.ReportAllocs()
			for b.Loop() {
				set := NewSubjectSet()
				set.MustAdd(wildcard)
				set.SubtractAll(removing)
			}
		})
	}
}
