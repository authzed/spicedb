package singleflight

type Group struct{}

func (g *Group) Do(key string, fn func() (any, error)) (any, error, bool) {
	v, err := fn()
	return v, err, false
}
