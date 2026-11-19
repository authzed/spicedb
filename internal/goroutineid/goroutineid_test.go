package goroutineid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCurrent(t *testing.T) {
	id := MustCurrent()
	require.Positive(t, id)
	require.Equal(t, id, MustCurrent())

	other := make(chan uint64)
	go func() { other <- MustCurrent() }()
	require.NotEqual(t, id, <-other)
}
