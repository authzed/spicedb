package secrets

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var tests = []uint8{
	0, 1, 2, 4, 8, 16, 128, 255,
}

func TestTokenBytes(t *testing.T) {
	assert := assert.New(t)
	for _, nbytes := range tests {
		res, err := TokenBytes(nbytes)
		require.NoError(t, err)
		assert.Len(res, int(nbytes))
	}
}

func TestTokenHex(t *testing.T) {
	assert := assert.New(t)
	for _, nbytes := range tests {
		res, err := TokenHex(nbytes)
		require.NoError(t, err)
		assert.Len(res, int(nbytes)*2)
	}
}
