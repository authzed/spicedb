package export

type Relation struct {
	Name string `json:"name"`
}

type Permission struct {
	Name string `json:"name"`
}

type Object struct {
	Name        string        `json:"name"`
	Namespace   string        `json:"namespace"`
	Relations   []*Relation   `json:"relations"`
	Permissions []*Permission `json:"permissions"`
}

type Schema struct {
	Objects []*Object `json:"objects"`
}
