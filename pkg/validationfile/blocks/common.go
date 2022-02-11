package blocks

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

// SourcePosition is a position in the input source.
type SourcePosition struct {
	// LineNumber is the 1-indexed line number in the input source.
	LineNumber int

	// ColumnPosition is the 1-indexed column position in the input source.
	ColumnPosition int
}

// ErrorWithSource is an error that includes the source text and position
// information.
type ErrorWithSource struct {
	error

	// Source is the source text for the error.
	Source string

	// LineNumber is the (1-indexed) line number of the error, or 0 if unknown.
	LineNumber uint64

	// ColumnPosition is the (1-indexed) column position of the error, or 0 if
	// unknown.
	ColumnPosition uint64
}

var (
	yamlLineRegex      = regexp.MustCompile(`line ([0-9]+): (.+)`)
	yamlUnmarshalRegex = regexp.MustCompile("cannot unmarshal !!str `([^`]+)...`")
)

func convertYamlError(err error) error {
	linePieces := yamlLineRegex.FindStringSubmatch(err.Error())
	if len(linePieces) == 3 {
		lineNumber, parseErr := strconv.ParseUint(linePieces[1], 10, 32)
		if parseErr != nil {
			lineNumber = 0
		}

		message := linePieces[2]
		source := ""
		unmarshalPieces := yamlUnmarshalRegex.FindStringSubmatch(message)
		if len(unmarshalPieces) == 2 {
			source = unmarshalPieces[1]
			message = fmt.Sprintf("unexpected value `%s`", unmarshalPieces[1])
		}

		return ErrorWithSource{
			errors.New(message),
			source,
			lineNumber,
			0,
		}
	}

	return err
}
