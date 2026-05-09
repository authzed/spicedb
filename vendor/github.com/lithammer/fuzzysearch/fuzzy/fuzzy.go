// Fuzzy searching allows for flexibly matching a string with partial input,
// useful for filtering data very quickly based on lightweight user input.
package fuzzy

import (
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func noopTransformer() transform.Transformer {
	return nopTransformer{}
}

func foldTransformer() transform.Transformer {
	return unicodeFoldTransformer{}
}

func normalizeTransformer() transform.Transformer {
	return transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
}

func normalizedFoldTransformer() transform.Transformer {
	return transform.Chain(normalizeTransformer(), foldTransformer())
}

// Match returns true if source matches target using a fuzzy-searching
// algorithm. Note that it doesn't implement Levenshtein distance (see
// RankMatch instead), but rather a simplified version where there's no
// approximation. The method will return true only if each character in the
// source can be found in the target and occurs after the preceding matches.
func Match(source, target string) bool {
	return match(source, target, noopTransformer())
}

// MatchFold is a case-insensitive version of Match.
func MatchFold(source, target string) bool {
	return match(source, target, foldTransformer())
}

// MatchNormalized is a unicode-normalized version of Match.
func MatchNormalized(source, target string) bool {
	return match(source, target, normalizeTransformer())
}

// MatchNormalizedFold is a unicode-normalized and case-insensitive version of Match.
func MatchNormalizedFold(source, target string) bool {
	return match(source, target, normalizedFoldTransformer())
}

func match(source, target string, transformer transform.Transformer) bool {
	sourceT := stringTransform(source, transformer)
	targetT := stringTransform(target, transformer)
	return matchTransformed(sourceT, targetT)
}

func matchTransformed(source, target string) bool {
	lenDiff := len(target) - len(source)

	if lenDiff < 0 {
		return false
	}

	if lenDiff == 0 && source == target {
		return true
	}

Outer:
	for _, r1 := range source {
		for i, r2 := range target {
			if r1 == r2 {
				target = target[i+utf8.RuneLen(r2):]
				continue Outer
			}
		}
		return false
	}

	return true
}

// Find will return a list of strings in targets that fuzzy matches source.
func Find(source string, targets []string) []string {
	return find(source, targets, noopTransformer())
}

// FindFold is a case-insensitive version of Find.
func FindFold(source string, targets []string) []string {
	return find(source, targets, foldTransformer())
}

// FindNormalized is a unicode-normalized version of Find.
func FindNormalized(source string, targets []string) []string {
	return find(source, targets, normalizeTransformer())
}

// FindNormalizedFold is a unicode-normalized and case-insensitive version of Find.
func FindNormalizedFold(source string, targets []string) []string {
	return find(source, targets, normalizedFoldTransformer())
}

func find(source string, targets []string, transformer transform.Transformer) []string {
	sourceT := stringTransform(source, transformer)

	var matches []string

	for _, target := range targets {
		targetT := stringTransform(target, transformer)
		if matchTransformed(sourceT, targetT) {
			matches = append(matches, target)
		}
	}

	return matches
}

// RankMatch is similar to Match except it will measure the Levenshtein
// distance between the source and the target and return its result. If there
// was no match, it will return -1.
// Given the requirements of match, RankMatch only needs to perform a subset of
// the Levenshtein calculation, only deletions need be considered, required
// additions and substitutions would fail the match test.
func RankMatch(source, target string) int {
	return rank(source, target, noopTransformer())
}

// RankMatchFold is a case-insensitive version of RankMatch.
func RankMatchFold(source, target string) int {
	return rank(source, target, foldTransformer())
}

// RankMatchNormalized is a unicode-normalized version of RankMatch.
func RankMatchNormalized(source, target string) int {
	return rank(source, target, normalizeTransformer())
}

// RankMatchNormalizedFold is a unicode-normalized and case-insensitive version of RankMatch.
func RankMatchNormalizedFold(source, target string) int {
	return rank(source, target, normalizedFoldTransformer())
}

func rank(source, target string, transformer transform.Transformer) int {
	lenDiff := len(target) - len(source)

	if lenDiff < 0 {
		return -1
	}

	source = stringTransform(source, transformer)
	target = stringTransform(target, transformer)

	if lenDiff == 0 && source == target {
		return 0
	}

	runeDiff := 0

Outer:
	for _, r1 := range source {
		for i, r2 := range target {
			if r1 == r2 {
				target = target[i+utf8.RuneLen(r2):]
				continue Outer
			} else {
				runeDiff++
			}
		}
		return -1
	}

	// Count up remaining char
	runeDiff += utf8.RuneCountInString(target)

	return runeDiff
}

// RankFind is similar to Find, except it will also rank all matches using
// Levenshtein distance.
func RankFind(source string, targets []string) Ranks {
	return rankFind(source, targets, noopTransformer())
}

// RankFindFold is a case-insensitive version of RankFind.
func RankFindFold(source string, targets []string) Ranks {
	return rankFind(source, targets, foldTransformer())
}

// RankFindNormalized is a unicode-normalized version of RankFind.
func RankFindNormalized(source string, targets []string) Ranks {
	return rankFind(source, targets, normalizeTransformer())
}

// RankFindNormalizedFold is a unicode-normalized and case-insensitive version of RankFind.
func RankFindNormalizedFold(source string, targets []string) Ranks {
	return rankFind(source, targets, normalizedFoldTransformer())
}

func rankFind(source string, targets []string, transformer transform.Transformer) Ranks {
	sourceT := stringTransform(source, transformer)

	var r Ranks

	for index, target := range targets {
		targetT := stringTransform(target, transformer)
		if matchTransformed(sourceT, targetT) {
			distance := LevenshteinDistance(source, target)
			r = append(r, Rank{source, target, distance, index})
		}
	}
	return r
}

type Rank struct {
	// Source is used as the source for matching.
	Source string

	// Target is the word matched against.
	Target string

	// Distance is the Levenshtein distance between Source and Target.
	Distance int

	// Location of Target in original list
	OriginalIndex int
}

type Ranks []Rank

func (r Ranks) Len() int {
	return len(r)
}

func (r Ranks) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r Ranks) Less(i, j int) bool {
	return r[i].Distance < r[j].Distance
}

func stringTransform(s string, t transform.Transformer) (transformed string) {
	// Fast path for the nop transformer to prevent unnecessary allocations.
	if _, ok := t.(nopTransformer); ok {
		return s
	}

	var err error
	transformed, _, err = transform.String(t, s)
	if err != nil {
		transformed = s
	}

	return
}

type unicodeFoldTransformer struct{ transform.NopResetter }

func (unicodeFoldTransformer) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	// Converting src to a string allocates.
	// In theory, it need not; see https://go.dev/issue/27148.
	// It is possible to write this loop using utf8.DecodeRune
	// and thereby avoid allocations, but it is noticeably slower.
	// So just let's wait for the compiler to get smarter.
	for _, r := range string(src) {
		if r == utf8.RuneError {
			// Go spec for ranging over a string says:
			// If the iteration encounters an invalid UTF-8 sequence,
			// the second value will be 0xFFFD, the Unicode replacement character,
			// and the next iteration will advance a single byte in the string.
			nSrc++
		} else {
			nSrc += utf8.RuneLen(r)
		}
		r = unicode.ToLower(r)
		x := utf8.RuneLen(r)
		if x > len(dst[nDst:]) {
			err = transform.ErrShortDst
			break
		}
		nDst += utf8.EncodeRune(dst[nDst:], r)
	}
	return nDst, nSrc, err
}

type nopTransformer struct{ transform.NopResetter }

func (nopTransformer) Transform(dst []byte, src []byte, atEOF bool) (int, int, error) {
	return 0, len(src), nil
}
