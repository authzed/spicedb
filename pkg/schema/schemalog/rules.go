package schemalog

type Rule interface {
	ResourceType() string
	SubjectTypes() []string
}

type TypeRule struct {
	Type string
}

type RelationRule struct {
	Type     string
	Relation string
}

type ExpansionRule struct {
	EdgeName string
}

type ArrowRule struct {
	Type     string
	EdgeName string
}

type AllRule struct {
	Type     string
	EdgeName string
}
