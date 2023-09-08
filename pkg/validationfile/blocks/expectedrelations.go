package blocks

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	yamlv3 "gopkg.in/yaml.v3"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ParsedExpectedRelations represents the expected relations defined in the validation
// file.
type ParsedExpectedRelations struct {
	// ValidationMap is the parsed expected relations validation map.
	ValidationMap ValidationMap

	// SourcePosition is the position of the expected relations in the file.
	SourcePosition spiceerrors.SourcePosition
}

// UnmarshalYAML is a custom unmarshaller.
func (per *ParsedExpectedRelations) UnmarshalYAML(node *yamlv3.Node) error {
	err := node.Decode(&per.ValidationMap)
	if err != nil {
		return convertYamlError(err)
	}

	per.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}

// ValidationMap is a map from an Object Relation (as a Relationship) to the
// validation strings containing the Subjects for that Object Relation.
type ValidationMap map[ObjectRelation][]ExpectedSubject

// ObjectRelation represents an ONR defined as a string in the key for
// the ValidationMap.
type ObjectRelation struct {
	// ObjectRelationString is the string form of the object relation.
	ObjectRelationString string

	// ObjectAndRelation is the parsed object and relation.
	ObjectAndRelation *core.ObjectAndRelation

	// SourcePosition is the position of the expected relations in the file.
	SourcePosition spiceerrors.SourcePosition
}

// UnmarshalYAML is a custom unmarshaller.
func (ors *ObjectRelation) UnmarshalYAML(node *yamlv3.Node) error {
	err := node.Decode(&ors.ObjectRelationString)
	if err != nil {
		return convertYamlError(err)
	}

	parsed := tuple.ParseONR(ors.ObjectRelationString)
	if parsed == nil {
		return spiceerrors.NewErrorWithSource(
			fmt.Errorf("could not parse %s", ors.ObjectRelationString),
			ors.ObjectRelationString,
			uint64(node.Line),
			uint64(node.Column),
		)
	}

	ors.ObjectAndRelation = parsed
	ors.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}

var (
	vsSubjectRegex                       = regexp.MustCompile(`(.*?)\[(?P<user_str>.*)](.*?)`)
	vsObjectAndRelationRegex             = regexp.MustCompile(`(.*?)<(?P<onr_str>[^>]+)>(.*?)`)
	vsSubjectWithExceptionsOrCaveatRegex = regexp.MustCompile(`^(?P<subject_onr>[^]\s]+)(?P<caveat>\[\.\.\.])?(\s+-\s+\{(?P<exceptions>[^}]+)})?$`)
)

// ExpectedSubject is a subject expected for the ObjectAndRelation.
type ExpectedSubject struct {
	// ValidationString holds a validation string containing a Subject and one or
	// more Relations to the parent Object.
	// Example: `[tenant/user:someuser#...] is <tenant/document:example#viewer>`
	ValidationString ValidationString

	// Subject is the subject expected. May be nil if not defined in the line.
	SubjectWithExceptions *SubjectWithExceptions

	// Resources are the resources under which the subject is found.
	Resources []*core.ObjectAndRelation

	// SourcePosition is the position of the expected subject in the file.
	SourcePosition spiceerrors.SourcePosition
}

// SubjectAndCaveat returns a subject and whether it is caveated.
type SubjectAndCaveat struct {
	// Subject is the subject found.
	Subject *core.ObjectAndRelation

	// IsCaveated indicates whether the subject is caveated.
	IsCaveated bool
}

// SubjectWithExceptions returns the subject found in a validation string, along with any exceptions.
type SubjectWithExceptions struct {
	// Subject is the subject found.
	Subject SubjectAndCaveat

	// Exceptions are those subjects removed from the subject, if it is a wildcard.
	Exceptions []SubjectAndCaveat
}

// UnmarshalYAML is a custom unmarshaller.
func (es *ExpectedSubject) UnmarshalYAML(node *yamlv3.Node) error {
	err := node.Decode(&es.ValidationString)
	if err != nil {
		return convertYamlError(err)
	}

	subjectWithExceptions, subErr := es.ValidationString.Subject()
	if subErr != nil {
		return spiceerrors.NewErrorWithSource(
			subErr,
			subErr.SourceCodeString,
			uint64(node.Line)+subErr.LineNumber,
			uint64(node.Column)+subErr.ColumnPosition,
		)
	}

	onrs, onrErr := es.ValidationString.ONRS()
	if onrErr != nil {
		return spiceerrors.NewErrorWithSource(
			onrErr,
			onrErr.SourceCodeString,
			uint64(node.Line)+onrErr.LineNumber,
			uint64(node.Column)+onrErr.ColumnPosition,
		)
	}

	es.SubjectWithExceptions = subjectWithExceptions
	es.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	es.Resources = onrs
	return nil
}

// ValidationString holds a validation string containing a Subject and one or
// more Relations to the parent Object.
// Example: `[tenant/user:someuser#...] is <tenant/document:example#viewer>`
type ValidationString string

// SubjectString returns the subject contained in the ValidationString, if any.
func (vs ValidationString) SubjectString() (string, bool) {
	result := vsSubjectRegex.FindStringSubmatch(string(vs))
	if len(result) != 4 {
		return "", false
	}

	return result[2], true
}

// Subject returns the subject contained in the ValidationString, if any. If
// none, returns nil.
func (vs ValidationString) Subject() (*SubjectWithExceptions, *spiceerrors.ErrorWithSource) {
	subjectStr, ok := vs.SubjectString()
	if !ok {
		return nil, nil
	}

	subjectStr = strings.TrimSpace(subjectStr)
	groups := vsSubjectWithExceptionsOrCaveatRegex.FindStringSubmatch(subjectStr)
	if len(groups) == 0 {
		bracketedSubjectString := "[" + subjectStr + "]"
		return nil, spiceerrors.NewErrorWithSource(fmt.Errorf("invalid subject: `%s`", subjectStr), bracketedSubjectString, 0, 0)
	}

	subjectONRString := groups[slices.Index(vsSubjectWithExceptionsOrCaveatRegex.SubexpNames(), "subject_onr")]
	subjectONR := tuple.ParseSubjectONR(subjectONRString)
	if subjectONR == nil {
		return nil, spiceerrors.NewErrorWithSource(fmt.Errorf("invalid subject: `%s`", subjectONRString), subjectONRString, 0, 0)
	}

	exceptionsString := strings.TrimSpace(groups[slices.Index(vsSubjectWithExceptionsOrCaveatRegex.SubexpNames(), "exceptions")])
	var exceptions []SubjectAndCaveat

	if len(exceptionsString) > 0 {
		exceptionsStringsSlice := strings.Split(exceptionsString, ",")
		exceptions = make([]SubjectAndCaveat, 0, len(exceptionsStringsSlice))
		for _, exceptionString := range exceptionsStringsSlice {
			isCaveated := false
			if strings.HasSuffix(exceptionString, "[...]") {
				exceptionString = strings.TrimSuffix(exceptionString, "[...]")
				isCaveated = true
			}

			exceptionONR := tuple.ParseSubjectONR(strings.TrimSpace(exceptionString))
			if exceptionONR == nil {
				return nil, spiceerrors.NewErrorWithSource(fmt.Errorf("invalid subject: `%s`", exceptionString), exceptionString, 0, 0)
			}

			exceptions = append(exceptions, SubjectAndCaveat{exceptionONR, isCaveated})
		}
	}

	isCaveated := len(strings.TrimSpace(groups[slices.Index(vsSubjectWithExceptionsOrCaveatRegex.SubexpNames(), "caveat")])) > 0
	return &SubjectWithExceptions{SubjectAndCaveat{subjectONR, isCaveated}, exceptions}, nil
}

// ONRStrings returns the ONRs contained in the ValidationString, if any.
func (vs ValidationString) ONRStrings() []string {
	results := vsObjectAndRelationRegex.FindAllStringSubmatch(string(vs), -1)
	onrStrings := []string{}
	for _, result := range results {
		onrStrings = append(onrStrings, result[2])
	}
	return onrStrings
}

// ONRS returns the subject ONRs in the ValidationString, if any.
func (vs ValidationString) ONRS() ([]*core.ObjectAndRelation, *spiceerrors.ErrorWithSource) {
	onrStrings := vs.ONRStrings()
	onrs := []*core.ObjectAndRelation{}
	for _, onrString := range onrStrings {
		found := tuple.ParseONR(onrString)
		if found == nil {
			return nil, spiceerrors.NewErrorWithSource(fmt.Errorf("invalid resource and relation: `%s`", onrString), onrString, 0, 0)
		}

		onrs = append(onrs, found)
	}
	return onrs, nil
}

// ParseExpectedRelationsBlock parses the given contents as an expected relations block.
func ParseExpectedRelationsBlock(contents []byte) (*ParsedExpectedRelations, error) {
	per := ParsedExpectedRelations{}
	err := yamlv3.Unmarshal(contents, &per)
	if err != nil {
		return nil, convertYamlError(err)
	}
	return &per, nil
}
