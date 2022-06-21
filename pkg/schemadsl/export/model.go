package export

type Relationship struct {
	Name string `json:"name"`
}

type Permission struct {
	Name string `json:"name"`
}

type Object struct {
	Name          string          `json:"name"`
	Namespace     string          `json:"namespace"`
	Relationships []*Relationship `json:"relationships"`
	Permissions   []*Permission   `json:"permissions"`
}

type Schema struct {
	Objects []*Object `json:"objects"`
}
