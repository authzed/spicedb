package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/checkbaseline"
)

func TestInvalidProfile(t *testing.T) {
	var out bytes.Buffer
	require.Equal(t, 2, run(context.Background(), []string{"-profile=invalid", "-output=unused"}, &out, &out))
}

func TestListScalingDatasets(t *testing.T) {
	var out bytes.Buffer
	require.Equal(t, 0, run(context.Background(), []string{"-catalog=scaling", "-list", "-dataset=^sized/direct/background/1000000$"}, &out, &out))
	require.JSONEq(t, `["sized/direct/background/1000000"]`, out.String())
}

func TestArtifactPublication(t *testing.T) {
	for _, mode := range []string{"audit", "measure"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "result.json")
			args := []string{"-catalog=scaling", "-dataset=^generated/direct/small$", "-case=^hit$", "-repo-root=../..", "-profile=both", "-mode=" + mode, "-repetitions=1", "-output=" + path}
			var stdout, stderr bytes.Buffer
			require.Equal(t, 0, run(t.Context(), args, &stdout, &stderr), stderr.String())
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			artifact, err := checkbaseline.ReadArtifact(bytes.NewReader(data))
			require.NoError(t, err)
			require.Len(t, artifact.Results, 2)
			for _, result := range artifact.Results {
				require.True(t, result.Valid)
				require.Equal(t, "relationship work matched", result.Status)
			}
			if mode == "measure" {
				require.Equal(t, 10, artifact.Samples)
			}
			require.Equal(t, 2, run(t.Context(), args, &stdout, &stderr))
			preserved, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, data, preserved)
			require.Equal(t, 0, run(t.Context(), slices.Concat(args, []string{"-overwrite"}), &stdout, &stderr), stderr.String())
			leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".check-baseline-*"))
			require.NoError(t, err)
			require.Empty(t, leftovers)
		})
	}
}

func TestFailedAuditStillPublishesArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.json")
	var output bytes.Buffer
	require.Equal(t, 1, run(t.Context(), []string{"-catalog=scaling", "-dataset=^missing$", "-output=" + path}, &output, &output))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	artifact, err := checkbaseline.ReadArtifact(bytes.NewReader(data))
	require.NoError(t, err)
	require.Empty(t, artifact.Results)
	require.Contains(t, output.String(), "no check cases selected")
}

func TestCommandRejectsBadInputs(t *testing.T) {
	for _, args := range [][]string{
		{"-unknown"},
		{"-output=unused", "-dataset=["},
		{"-output=unused", "-case=["},
		{"-mode=invalid", "-output=unused"},
	} {
		var output bytes.Buffer
		require.NotZero(t, run(t.Context(), args, &output, &output))
		require.NotEmpty(t, output.String())
	}
}

func TestOutputFailure(t *testing.T) {
	var output bytes.Buffer
	path := filepath.Join(t.TempDir(), "missing", "result.json")
	require.Equal(t, 1, run(t.Context(), []string{"-catalog=scaling", "-dataset=^missing$", "-output=" + path}, &output, &output))
	require.Contains(t, output.String(), "no such file or directory")
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestListWriteFailure(t *testing.T) {
	var stderr bytes.Buffer
	require.Equal(t, 1, run(t.Context(), []string{"-catalog=scaling", "-list"}, failingWriter{}, &stderr))
}
