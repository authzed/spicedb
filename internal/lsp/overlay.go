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
type lspOverlayFS struct {
	base      fs.FS
	sourceDir string
	files     *persistent.Map[lsp.DocumentURI, trackedFile]
}

var _ fs.FS = &lspOverlayFS{}

func newLSPOverlayFS(sourceDir string, files *persistent.Map[lsp.DocumentURI, trackedFile]) fs.FS {
	return &lspOverlayFS{
		base:      os.DirFS(sourceDir),
		sourceDir: sourceDir,
		files:     files,
	}
}

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
