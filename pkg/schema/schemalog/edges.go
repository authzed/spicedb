package schemalog

type Edge interface {
	Entity
	Rule
	Definition() *Definition
	Rules() []Rule
}

var _ Edge = &Relation{}
var _ Edge = &Permission{}

type Relation struct {
	definition *Definition
	name       string
	body       []Rule
}

func (r *Relation) Name() string {
	return r.name
}

func (r *Relation) Definition() *Definition {
	return r.definition
}

type Permission struct {
	definition *Definition
	name       string
	body       []Rule
}

func (p *Permission) Name() string {
	return p.name
}

func (p *Permission) Definition() *Definition {
	return p.definition
}
