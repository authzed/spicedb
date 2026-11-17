package x509util

import (
	"bytes"
	"slices"
	"sync"
	"time"

	log "github.com/authzed/spicedb/internal/logging"
)

const defaultReloadInterval = 10 * time.Second

// reloading holds a value parsed from files on disk and re-reads those files
// on demand, at most once per interval. It runs no background goroutine.
type reloading[T any] struct {
	what  string
	path  string
	read  func() ([][]byte, error)
	parse func([][]byte) (T, error)

	mu       sync.Mutex
	interval time.Duration // GUARDED_BY(mu)
	value    T             // GUARDED_BY(mu)
	contents [][]byte      // GUARDED_BY(mu)
	lastRead time.Time     // GUARDED_BY(mu)
}

// newReloading reads and parses the files eagerly, so a missing or malformed
// file is reported here rather than on first use.
func newReloading[T any](what, path string, read func() ([][]byte, error), parse func([][]byte) (T, error)) (*reloading[T], error) {
	contents, err := read()
	if err != nil {
		return nil, err
	}

	value, err := parse(contents)
	if err != nil {
		return nil, err
	}

	return &reloading[T]{
		what:     what,
		path:     path,
		read:     read,
		parse:    parse,
		interval: defaultReloadInterval,
		value:    value,
		contents: contents,
		lastRead: time.Now(),
	}, nil
}

func (r *reloading[T]) setInterval(interval time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.interval = interval
}

// get returns the current value, re-reading the files if the interval has
// elapsed. A failed re-read keeps the previous value, so a file that is
// briefly absent or truncated partway through a rotation is not served.
func (r *reloading[T]) get() T {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if now.Sub(r.lastRead) < r.interval {
		return r.value
	}
	r.lastRead = now

	contents, err := r.read()
	if err != nil {
		log.Warn().Err(err).Str("path", r.path).Msg("failed to re-read " + r.what)
		return r.value
	}

	if slices.EqualFunc(r.contents, contents, bytes.Equal) {
		return r.value
	}

	value, err := r.parse(contents)
	if err != nil {
		log.Warn().Err(err).Str("path", r.path).Msg("failed to parse re-read " + r.what)
		return r.value
	}

	log.Info().Str("path", r.path).Msg("reloaded " + r.what)
	r.value = value
	r.contents = contents
	return r.value
}
