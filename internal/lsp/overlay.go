package lsp

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jzelinskie/persistent"
	"github.com/sourcegraph/go-lsp"
)

// lspOverlayFS is an fs.FS that serves open editor files from memory
// and falls back to disk for everything else. It is rooted at sourceDir.
//
// This is needed because the schema compiler expects an fs.FS, but the LSP
// server must feed it unsaved editor buffers (not the stale on-disk content).
// The overlay ensures diagnostics, hover, and other features reflect what the
// user is actively editing.
type lspOverlayFS struct {
	base      fs.FS
	sourceDir string
	files     *persistent.Map[lsp.DocumentURI, trackedFile]
}

var _ fs.FS = &lspOverlayFS{}

// newLSPOverlayFS returns an fs.FS rooted at sourceDir that reads open editor
// buffers from files and falls back to os.DirFS for files not held in memory.
func newLSPOverlayFS(sourceDir string, files *persistent.Map[lsp.DocumentURI, trackedFile]) fs.FS {
	return &lspOverlayFS{
		base:      os.DirFS(sourceDir),
		sourceDir: sourceDir,
		files:     files,
	}
}

// Open returns the in-memory editor buffer for name if one exists,
// otherwise it reads the file from disk.
func (o *lspOverlayFS) Open(name string) (fs.File, error) {
	absPath := filepath.Join(o.sourceDir, filepath.FromSlash(name))
	uri := lsp.DocumentURI("file://" + absPath)
	if file, ok := o.files.Get(uri); ok {
		return newMemFile(name, file.contents), nil
	}
	return o.base.Open(name)
}

// memFile implements fs.File for an in-memory string.
type memFile struct {
	name    string
	content string
	reader  *strings.Reader
}

var _ fs.File = &memFile{}

func newMemFile(name, content string) *memFile {
	return &memFile{name: name, content: content, reader: strings.NewReader(content)}
}

func (m *memFile) Read(b []byte) (int, error) { return m.reader.Read(b) }
func (m *memFile) Close() error               { return nil }
func (m *memFile) Stat() (fs.FileInfo, error) {
	return memFileInfo{name: m.name, size: int64(len(m.content))}, nil
}

type memFileInfo struct {
	name string
	size int64
}

func (i memFileInfo) Name() string       { return filepath.Base(i.name) }
func (i memFileInfo) Size() int64        { return i.size }
func (i memFileInfo) Mode() fs.FileMode  { return 0o444 }
func (i memFileInfo) ModTime() time.Time { return time.Time{} }
func (i memFileInfo) IsDir() bool        { return false }
func (i memFileInfo) Sys() any           { return nil }

// uriToSourceDir extracts the containing directory from a file:// URI.
func uriToSourceDir(uri lsp.DocumentURI) string {
	path := strings.TrimPrefix(string(uri), "file://")
	return filepath.Dir(path)
}

// resolveURI resolves a relative path against the directory of baseURI,
// returning a new file:// DocumentURI.
func resolveURI(baseURI lsp.DocumentURI, relativePath string) lsp.DocumentURI {
	return lsp.DocumentURI("file://" + filepath.Join(uriToSourceDir(baseURI), relativePath))
}
