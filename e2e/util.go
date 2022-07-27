package e2e

import (
	"io"
	"testing"
)

// TLogger wraps a testing.TB and makes it conform to io.Writer
type TLogger struct {
	testing.TB
}

// Write satisfied io.Writer
func (t *TLogger) Write(p []byte) (int, error) {
	t.Helper()
	t.Log(string(p))
	return len(p), nil
}

// NewTLog returns a TLogger
func NewTLog(t testing.TB) io.Writer {
	t.Helper()
	return &TLogger{TB: t}
}
