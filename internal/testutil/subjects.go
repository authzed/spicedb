package testutil

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// WrapFoundSubject wraps the given subject into a pointer to it, unless nil, in which case this method returns
// nil.
func WrapFoundSubject(sub *v1.FoundSubject) **v1.FoundSubject {
	if sub == nil {
		return nil
	}

	return &sub
}

// FoundSubject returns a FoundSubject with the given ID.
func FoundSubject(subjectID string) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId: subjectID,
	}
}

// CaveatedFoundSubject returns a FoundSubject with the given ID and caveat expression.
func CaveatedFoundSubject(subjectID string, expr *core.CaveatExpression) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        subjectID,
		CaveatExpression: expr,
	}
}

// CaveatedWildcard returns a wildcard FoundSubject with the given caveat expression and exclusions.
func CaveatedWildcard(expr *core.CaveatExpression, exclusions ...*v1.FoundSubject) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        tuple.PublicWildcard,
		ExcludedSubjects: exclusions,
		CaveatExpression: expr,
	}
}

// Wildcard returns a FoundSubject with the given subject IDs as concrete exclusions.
func Wildcard(exclusions ...string) *v1.FoundSubject {
	excludedSubjects := make([]*v1.FoundSubject, 0, len(exclusions))
	for _, excludedID := range exclusions {
		excludedSubjects = append(excludedSubjects, &v1.FoundSubject{
			SubjectId: excludedID,
		})
	}

	return &v1.FoundSubject{
		SubjectId:        tuple.PublicWildcard,
		ExcludedSubjects: excludedSubjects,
	}
}

// RequireEquivalentSets requires that the given sets of subjects are equivalent.
func RequireEquivalentSets(t *testing.T, expected []*v1.FoundSubject, found []*v1.FoundSubject) {
	t.Helper()
	err := CheckEquivalentSets(expected, found)
	require.NoError(t, err, "found different subject sets: %v \n %v", err, found)
}

// RequireExpectedSubject requires that the given expected and produced subjects match.
func RequireExpectedSubject(t *testing.T, expected *v1.FoundSubject, produced **v1.FoundSubject) {
	t.Helper()
	if expected == nil {
		require.Nil(t, produced)
	} else {
		require.NotNil(t, produced)

		found := *produced
		err := CheckEquivalentSubjects(expected, found)
		require.NoError(t, err, "found different subjects: %v", err)
	}
}

// CheckEquivalentSets checks if the sets of subjects are equivalent and returns an error if they are not.
func CheckEquivalentSets(expected []*v1.FoundSubject, found []*v1.FoundSubject) error {
	if len(expected) != len(found) {
		return fmt.Errorf("found mismatch in number of elements:\n\texpected: %s\n\tfound: %s", FormatSubjects(expected), FormatSubjects(found))
	}

	slices.SortFunc(expected, CmpSubjects)
	slices.SortFunc(found, CmpSubjects)

	for index := range expected {
		err := CheckEquivalentSubjects(expected[index], found[index])
		if err != nil {
			return fmt.Errorf("found mismatch for subject #%d: %w", index, err)
		}
	}

	return nil
}

// CheckEquivalentSubjects checks if the given subjects are equivalent and returns an error if they are not.
func CheckEquivalentSubjects(expected *v1.FoundSubject, found *v1.FoundSubject) error {
	if expected.SubjectId != found.SubjectId {
		return fmt.Errorf("expected subject %s, found %s", expected.SubjectId, found.SubjectId)
	}

	err := CheckEquivalentSets(expected.ExcludedSubjects, found.ExcludedSubjects)
	if err != nil {
		return fmt.Errorf("difference in exclusions: %w", err)
	}

	return checkEquivalentCaveatExprs(expected.CaveatExpression, found.CaveatExpression)
}

// FormatSubjects formats the given slice of subjects in a human-readable string.
func FormatSubjects(subs []*v1.FoundSubject) string {
	formatted := make([]string, 0, len(subs))
	for _, sub := range subs {
		formatted = append(formatted, FormatSubject(sub))
	}
	return strings.Join(formatted, ",")
}

// FormatSubject formats the given subject (which can be nil) into a human-readable string.
func FormatSubject(sub *v1.FoundSubject) string {
	if sub == nil {
		return "[nil]"
	}

	if sub.GetSubjectId() == tuple.PublicWildcard {
		exclusions := make([]string, 0, len(sub.GetExcludedSubjects()))
		for _, excludedSubject := range sub.GetExcludedSubjects() {
			exclusions = append(exclusions, FormatSubject(excludedSubject))
		}

		exclusionsStr := ""
		if len(exclusions) > 0 {
			exclusionsStr = fmt.Sprintf("- {%s}", strings.Join(exclusions, ","))
		}

		if sub.GetCaveatExpression() != nil {
			return fmt.Sprintf("{*%s}[%s]", exclusionsStr, formatCaveatExpr(sub.GetCaveatExpression()))
		}

		return fmt.Sprintf("{*%s}", exclusionsStr)
	}

	if sub.GetCaveatExpression() != nil {
		return fmt.Sprintf("%s[%s]", sub.GetSubjectId(), formatCaveatExpr(sub.GetCaveatExpression()))
	}

	return sub.GetSubjectId()
}

// formatCaveatExpr formats a caveat expression (which can be nil) into a human readable string.
func formatCaveatExpr(expr *core.CaveatExpression) string {
	if expr == nil {
		return "[nil]"
	}

	if expr.GetCaveat() != nil {
		return expr.GetCaveat().CaveatName
	}

	switch expr.GetOperation().Op {
	case core.CaveatOperation_AND:
		return fmt.Sprintf("(%s) && (%s)",
			formatCaveatExpr(expr.GetOperation().GetChildren()[0]),
			formatCaveatExpr(expr.GetOperation().GetChildren()[1]),
		)

	case core.CaveatOperation_OR:
		return fmt.Sprintf("(%s) || (%s)",
			formatCaveatExpr(expr.GetOperation().GetChildren()[0]),
			formatCaveatExpr(expr.GetOperation().GetChildren()[1]),
		)

	case core.CaveatOperation_NOT:
		return fmt.Sprintf("!(%s)",
			formatCaveatExpr(expr.GetOperation().GetChildren()[0]),
		)

	default:
		panic("unknown op")
	}
}

// checkEquivalentCaveatExprs checks if the given caveat expressions are equivalent and returns an error if they are not.
func checkEquivalentCaveatExprs(expected *core.CaveatExpression, found *core.CaveatExpression) error {
	if expected == nil {
		if found != nil {
			return fmt.Errorf("found non-nil caveat expression `%s` where expected nil", formatCaveatExpr(found))
		}
		return nil
	}

	if found == nil {
		if expected != nil {
			return fmt.Errorf("expected non-nil caveat expression `%s` where found nil", formatCaveatExpr(expected))
		}
		return nil
	}

	// Caveat expressions that the subjectset generates can be different in structure but *logically* equivalent,
	// so we compare by building a boolean table for each referenced caveat name and then checking all combinations
	// of boolean inputs to ensure the expressions produce the same output. Note that while this isn't the most
	// efficient means of comparison, it is logically correct.
	referencedNamesSet := mapz.NewSet[string]()
	collectReferencedNames(expected, referencedNamesSet)
	collectReferencedNames(found, referencedNamesSet)

	referencedNames := referencedNamesSet.AsSlice()
	for _, values := range combinatorialValues(referencedNames) {
		expectedResult, err := executeCaveatExprForTesting(expected, values)
		if err != nil {
			return err
		}

		foundResult, err := executeCaveatExprForTesting(found, values)
		if err != nil {
			return err
		}

		if expectedResult != foundResult {
			return fmt.Errorf("found difference between caveats for values:\n\tvalues: %v\n\texpected caveat: %s\n\tfound caveat:%s", values, formatCaveatExpr(expected), formatCaveatExpr(found))
		}
	}
	return nil
}

// executeCaveatExprForTesting "executes" the given caveat expression for testing. DO NOT USE OUTSIDE OF TESTING.
// This method *ignores* caveat context and treats each caveat as just its name.
func executeCaveatExprForTesting(expr *core.CaveatExpression, values map[string]bool) (bool, error) {
	if expr.GetCaveat() != nil {
		return values[expr.GetCaveat().CaveatName], nil
	}

	switch expr.GetOperation().Op {
	case core.CaveatOperation_AND:
		if len(expr.GetOperation().Children) != 2 {
			return false, spiceerrors.MustBugf("found invalid child count for AND")
		}

		left, err := executeCaveatExprForTesting(expr.GetOperation().Children[0], values)
		if err != nil {
			return false, err
		}

		right, err := executeCaveatExprForTesting(expr.GetOperation().Children[1], values)
		if err != nil {
			return false, err
		}

		return left && right, nil

	case core.CaveatOperation_OR:
		if len(expr.GetOperation().Children) != 2 {
			return false, spiceerrors.MustBugf("found invalid child count for OR")
		}

		left, err := executeCaveatExprForTesting(expr.GetOperation().Children[0], values)
		if err != nil {
			return false, err
		}

		right, err := executeCaveatExprForTesting(expr.GetOperation().Children[1], values)
		if err != nil {
			return false, err
		}

		return left || right, nil

	case core.CaveatOperation_NOT:
		if len(expr.GetOperation().Children) != 1 {
			return false, spiceerrors.MustBugf("found invalid child count for NOT")
		}

		result, err := executeCaveatExprForTesting(expr.GetOperation().Children[0], values)
		if err != nil {
			return false, err
		}
		return !result, nil

	default:
		return false, spiceerrors.MustBugf("unknown caveat operation")
	}
}

// combinatorialValues returns the combinatorial set of values where each name is either true and false.
func combinatorialValues(names []string) []map[string]bool {
	if len(names) == 0 {
		return nil
	}

	name := names[0]
	childMaps := combinatorialValues(names[1:])

	cmaps := make([]map[string]bool, 0, len(childMaps)*2)
	if len(childMaps) == 0 {
		for _, v := range []bool{true, false} {
			cloned := map[string]bool{}
			cloned[name] = v
			cmaps = append(cmaps, cloned)
		}
	} else {
		for _, childMap := range childMaps {
			for _, v := range []bool{true, false} {
				cloned := maps.Clone(childMap)
				cloned[name] = v
				cmaps = append(cmaps, cloned)
			}
		}
	}

	return cmaps
}

// collectReferencedNames collects all referenced caveat names into the given set.
func collectReferencedNames(expr *core.CaveatExpression, nameSet *mapz.Set[string]) {
	if expr.GetCaveat() != nil {
		nameSet.Insert(expr.GetCaveat().CaveatName)
		return
	}

	for _, child := range expr.GetOperation().GetChildren() {
		collectReferencedNames(child, nameSet)
	}
}

// CmpSubjects compares FoundSubjects such that they can be sorted.
func CmpSubjects(a, b *v1.FoundSubject) int {
	return cmp.Compare(a.SubjectId, b.SubjectId)
}
