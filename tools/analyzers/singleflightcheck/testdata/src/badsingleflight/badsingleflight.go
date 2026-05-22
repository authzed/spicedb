package badsingleflight

import (
	"context"

	"golang.org/x/sync/singleflight"
)

var group singleflight.Group

func HasContext(ctx context.Context) {
	group.Do("key", func() (any, error) { // want "use resenje.org/singleflight instead of golang.org/x/sync/singleflight in functions with context.Context"
		return nil, nil
	})
}
