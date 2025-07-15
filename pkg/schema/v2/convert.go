package schema

import corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"

func convertDefinition(def *corev1.NamespaceDefinition) (*Definition, error) {
	out := &Definition{
		Name:        def.GetName(),
		Relations:   make(map[string]Relation),
		Permissions: make(map[string]Permission),
	}
	for _, r := range def.GetRelation() {
		if userset := r.GetUsersetRewrite(); userset != nil {
			perm, err := convertUserset(userset)
			if err != nil {
				return nil, err
			}
			out.Permissions[r.GetName()] = perm
		} else if typeinfo := r.GetTypeInformation(); typeinfo != nil {
			rel, err := convertTypeInformation(typeinfo)
			if err != nil {
				return nil, err
			}
			out.Relations[r.GetName()] = rel
		}
	}
	return out, nil
}

func convertCaveat(def *corev1.CaveatDefinition) (*Caveat, error) {
	return &Caveat{}, nil
}

func convertUserset(userset *corev1.UsersetRewrite) (Permission, error) {
	return Permission{}, nil
}

func convertTypeInformation(typeinfo *corev1.TypeInformation) (Relation, error) {
	return Relation{}, nil
}
