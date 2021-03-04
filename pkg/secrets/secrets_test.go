package secrets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var tests = []uint8{
	0, 1, 2, 4, 8, 16, 128, 255,
}

func TestTokenBytes(t *testing.T) {
	assert := assert.New(t)
	for _, nbytes := range tests {
		res, err := TokenBytes(nbytes)
		assert.Nil(err)
		assert.Equal(int(nbytes), len(res))
	}
}

func TestTokenHex(t *testing.T) {
	assert := assert.New(t)
	for _, nbytes := range tests {
		res, err := TokenHex(nbytes)
		assert.Nil(err)
		assert.Equal(int(nbytes)*2, len(res))
	}
}
