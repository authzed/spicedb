package goodsingleflight

import (
	"golang.org/x/sync/singleflight"
)

var group singleflight.Group

func NoContext() {
	group.Do("key", func() (any, error) {
		return nil, nil
	})
}
