package lsp

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jzelinskie/persistent"
	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/require"
)

func TestOverlayFSReturnsInMemoryContents(t *testing.T) {
	dir := t.TempDir()
	diskContent := "disk content"
	memContent := "in-memory content"

	// Write a file to disk.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.zed"), []byte(diskContent), 0o600))

	// Build the in-memory file map with different content for the same file.
	uri := lsp.DocumentURI("file://" + filepath.Join(dir, "test.zed"))
	files := persistent.NewMap[lsp.DocumentURI, trackedFile](func(x, y lsp.DocumentURI) bool {
		return string(x) < string(y)
	})
	defer files.Destroy()
	files.Set(uri, trackedFile{contents: memContent}, nil)

	// Create the overlay FS and open the file.
	overlay := newLSPOverlayFS(dir, files)
	f, err := overlay.Open("test.zed")
	require.NoError(t, err)
	defer f.Close()

	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, memContent, string(got))

	f.Close()

	// Now delete the in-memory content, it should fetch from disk.
	files.Delete(uri)

	f2, err := overlay.Open("test.zed")
	require.NoError(t, err)
	defer f2.Close()
	got, err = io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, diskContent, string(got))
}

func TestOverlayFSRejectsPathOutsideSourceDir(t *testing.T) {
	dir := t.TempDir()

	files := persistent.NewMap[lsp.DocumentURI, trackedFile](func(x, y lsp.DocumentURI) bool {
		return string(x) < string(y)
	})
	defer files.Destroy()

	overlay := newLSPOverlayFS(filepath.Join(dir, "subdir"), files)
	_, err := overlay.Open("../secret.zed")
	require.Error(t, err)
}

func TestURIToSourceDir(t *testing.T) {
	tests := []struct {
		uri      lsp.DocumentURI
		expected string
	}{
		{"file:///home/user/project/schema.zed", "/home/user/project"},
		{"file:///home/user/project/sub/schema.zed", "/home/user/project/sub"},
		{"file:///schema.zed", "/"},
	}

	for _, tt := range tests {
		t.Run(string(tt.uri), func(t *testing.T) {
			require.Equal(t, tt.expected, uriToSourceDir(tt.uri))
		})
	}
}

func TestResolveURI(t *testing.T) {
	tests := []struct {
		name     string
		baseURI  lsp.DocumentURI
		relative string
		expected lsp.DocumentURI
	}{
		{
			"1",
			"file:///home/user/project/schema.zed",
			"other.zed",
			"file:///home/user/project/other.zed",
		},
		{
			"2",
			"file:///home/user/project/schema.zed",
			"sub/other.zed",
			"file:///home/user/project/sub/other.zed",
		},
		{
			"3",
			"file:///home/user/project/sub/schema.zed",
			"../other.zed",
			"file:///home/user/project/other.zed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, resolveURI(tt.baseURI, tt.relative))
		})
	}
}
