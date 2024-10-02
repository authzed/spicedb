package testutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var (
	caveatexpr   = caveats.CaveatExprForTesting
	caveatAnd    = caveats.And
	caveatOr     = caveats.Or
	caveatInvert = caveats.Invert
	sub          = FoundSubject
	wc           = Wildcard
	csub         = CaveatedFoundSubject
	cwc          = CaveatedWildcard
)

func TestCompareSubjects(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		first              *v1.FoundSubject
		second             *v1.FoundSubject
		expectedEquivalent bool
	}{
		{
			sub("1"),
			sub("1"),
			true,
		},
		{
			wc(),
			wc(),
			true,
		},
		{
			wc("1"),
			wc("1"),
			true,
		},
		{
			wc("1", "2"),
			wc("2", "1"),
			true,
		},
		{
			wc("1", "2", "3"),
			wc("2", "1"),
			false,
		},
		{
			sub("1"),
			sub("2"),
			false,
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatInvert(caveatexpr("first"))),
			false,
		},
		{
			csub("1", caveatInvert(caveatexpr("first"))),
			csub("1", caveatInvert(caveatexpr("first"))),
			true,
		},
		{
			csub("1", caveatInvert(caveatexpr("first"))),
			csub("1", caveatInvert(caveatexpr("second"))),
			false,
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatexpr("first")),
			true,
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatexpr("second")),
			false,
		},
		{
			cwc(caveatexpr("first")),
			cwc(caveatexpr("first")),
			true,
		},
		{
			cwc(caveatexpr("first")),
			cwc(caveatexpr("second")),
			false,
		},
		{
			cwc(caveatexpr("first"), csub("1", caveatexpr("c1"))),
			cwc(caveatexpr("first"), csub("1", caveatexpr("c1"))),
			true,
		},
		{
			cwc(caveatexpr("first"), csub("1", caveatexpr("c1"))),
			cwc(caveatexpr("first"), csub("1", caveatexpr("c2"))),
			false,
		},
		{
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("second"),
				),
			),
			cwc(
				caveatAnd(
					caveatexpr("second"),
					caveatexpr("first"),
				),
			),
			true,
		},
		{
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("second"),
				),
			),
			cwc(
				caveatOr(
					caveatexpr("second"),
					caveatexpr("first"),
				),
			),
			false,
		},
		{
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("first"),
				),
			),
			cwc(
				caveatexpr("first"),
			),
			true,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%s vs %s", FormatSubject(tc.first), FormatSubject(tc.second)), func(t *testing.T) {
			err := CheckEquivalentSubjects(tc.first, tc.second)
			if tc.expectedEquivalent {
				require.NoError(t, err)
			} else {
				require.NotNil(t, err)
			}

			err = CheckEquivalentSets([]*v1.FoundSubject{tc.first}, []*v1.FoundSubject{tc.second})
			if tc.expectedEquivalent {
				require.NoError(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}
