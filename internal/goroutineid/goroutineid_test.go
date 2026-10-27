package goroutineid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCurrent(t *testing.T) {
	id := Current()
	require.Positive(t, id)
	require.Equal(t, id, Current())

	other := make(chan uint64)
	go func() { other <- Current() }()
	require.NotEqual(t, id, <-other)
}
