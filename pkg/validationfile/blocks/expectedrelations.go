package blocks

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	yamlv3 "gopkg.in/yaml.v3"

	"github.com/ccoveille/go-safecast"

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
	ObjectAndRelation tuple.ObjectAndRelation

	// SourcePosition is the position of the expected relations in the file.
	SourcePosition spiceerrors.SourcePosition
}

// UnmarshalYAML is a custom unmarshaller.
func (ors *ObjectRelation) UnmarshalYAML(node *yamlv3.Node) error {
	err := node.Decode(&ors.ObjectRelationString)
	if err != nil {
		return convertYamlError(err)
	}

	line, err := safecast.ToUint64(node.Line)
	if err != nil {
		return err
	}
	column, err := safecast.ToUint64(node.Column)
	if err != nil {
		return err
	}

	parsed, err := tuple.ParseONR(ors.ObjectRelationString)
	if err != nil {
		return spiceerrors.NewWithSourceError(
			fmt.Errorf("could not parse %s: %w", ors.ObjectRelationString, err),
			ors.ObjectRelationString,
			line,
			column,
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
	Resources []tuple.ObjectAndRelation

	// SourcePosition is the position of the expected subject in the file.
	SourcePosition spiceerrors.SourcePosition
}

// SubjectAndCaveat returns a subject and whether it is caveated.
type SubjectAndCaveat struct {
	// Subject is the subject found.
	Subject tuple.ObjectAndRelation

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

	line, err := safecast.ToUint64(node.Line)
	if err != nil {
		return err
	}
	column, err := safecast.ToUint64(node.Column)
	if err != nil {
		return err
	}

	subjectWithExceptions, subErr := es.ValidationString.Subject()
	if subErr != nil {
		return spiceerrors.NewWithSourceError(
			subErr,
			subErr.SourceCodeString,
			line+subErr.LineNumber,
			column+subErr.ColumnPosition,
		)
	}

	onrs, onrErr := es.ValidationString.ONRS()
	if onrErr != nil {
		return spiceerrors.NewWithSourceError(
			onrErr,
			onrErr.SourceCodeString,
			line+onrErr.LineNumber,
			column+onrErr.ColumnPosition,
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
func (vs ValidationString) Subject() (*SubjectWithExceptions, *spiceerrors.WithSourceError) {
	subjectStr, ok := vs.SubjectString()
	if !ok {
		return nil, nil
	}

	subjectStr = strings.TrimSpace(subjectStr)
	groups := vsSubjectWithExceptionsOrCaveatRegex.FindStringSubmatch(subjectStr)
	if len(groups) == 0 {
		bracketedSubjectString := "[" + subjectStr + "]"
		return nil, spiceerrors.NewWithSourceError(fmt.Errorf("invalid subject: `%s`", subjectStr), bracketedSubjectString, 0, 0)
	}

	subjectONRString := groups[slices.Index(vsSubjectWithExceptionsOrCaveatRegex.SubexpNames(), "subject_onr")]
	subjectONR, err := tuple.ParseSubjectONR(subjectONRString)
	if err != nil {
		return nil, spiceerrors.NewWithSourceError(fmt.Errorf("invalid subject: `%s`: %w", subjectONRString, err), subjectONRString, 0, 0)
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

			exceptionONR, err := tuple.ParseSubjectONR(strings.TrimSpace(exceptionString))
			if err != nil {
				return nil, spiceerrors.NewWithSourceError(fmt.Errorf("invalid subject: `%s`: %w", exceptionString, err), exceptionString, 0, 0)
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
func (vs ValidationString) ONRS() ([]tuple.ObjectAndRelation, *spiceerrors.WithSourceError) {
	onrStrings := vs.ONRStrings()
	onrs := []tuple.ObjectAndRelation{}
	for _, onrString := range onrStrings {
		found, err := tuple.ParseONR(onrString)
		if err != nil {
			return nil, spiceerrors.NewWithSourceError(fmt.Errorf("invalid resource and relation: `%s`: %w", onrString, err), onrString, 0, 0)
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
