package e2e

import (
	"io"
	"testing"
)

type TLogger struct {
	testing.TB
}

func (t *TLogger) Write(p []byte) (int, error) {
	t.Log(string(p))
	return len(p), nil
}

func TLog(t testing.TB) io.Writer {
	return &TLogger{TB: t}
}
