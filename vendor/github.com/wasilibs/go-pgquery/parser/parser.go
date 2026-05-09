//go:build !pgquery_cgo && !tinygo

package parser

import "github.com/wasilibs/go-pgquery/internal/errors"

type Error = errors.Error
