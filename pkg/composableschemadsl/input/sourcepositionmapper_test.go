package input

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionMapping(t *testing.T) {
	mappingText, err := os.ReadFile("tests/mapping.txt")
	require.NoError(t, err, "Got error reading mapping file")

	mapper := CreateSourcePositionMapper(mappingText)

	for runePosition := range mappingText {
		lineNumber, colPosition, err := mapper.RunePositionToLineAndCol(runePosition)
		require.NoError(t, err, "Got error mapping file")

		// Check mapping back.
		foundRunePosition, err := mapper.LineAndColToRunePosition(lineNumber, colPosition)
		require.NoError(t, err, "Got error mapping file")

		if !assert.Equal(t, runePosition, foundRunePosition, "Rune position mismatch for position %v", runePosition) {
			return
		}
	}
}
