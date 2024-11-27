package blocks

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

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
			if strings.Contains(source, " ") {
				source, _, _ = strings.Cut(source, " ")
			}

			message = fmt.Sprintf("unexpected value `%s`", source)
		}

		return spiceerrors.NewWithSourceError(
			errors.New(message),
			source,
			lineNumber,
			0,
		)
	}

	return err
}
