//go:build wasip1

package main

// Must be initialized before any dependency that reads os.Args; see the package docs.
import _ "github.com/authzed/spicedb/pkg/cmd/testserver/wasi/internal/argv"
