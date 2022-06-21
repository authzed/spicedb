package export

import (
	"encoding/json"
	"fmt"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"io"
	"strings"
)

func WriteSchemaTo(definition []*corev1.NamespaceDefinition, w io.Writer) error {

	var objects []*Object
	for _, def := range definition {
		o, err := mapDefinition(def)
		if err != nil {
			return fmt.Errorf("failed to export %q: %w", def.Name, err)
		}
		objects = append(objects, o)
	}

	data, err := json.Marshal(objects)
	if err != nil {
		return fmt.Errorf("unable to serialize schema for export: %w", err)
	}

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("unable to write schema for export: %w", err)
	}
	return nil
}

func mapDefinition(def *corev1.NamespaceDefinition) (*Object, error) {

	relations := []*Relationship{}
	permissions := []*Permission{}
	for _, r := range def.Relation {
		//TODO: Is there a better way to distinguish the two?
		if r.TypeInformation != nil && r.UsersetRewrite == nil {
			relations = append(relations, mapRelation(r))
		} else if r.TypeInformation == nil && r.UsersetRewrite != nil {
			permissions = append(permissions, mapPermission(r))
		} else {
			return nil, fmt.Errorf("unexpected relationship: %s", r.Name)
		}
	}

	splits := strings.SplitN(def.Name, "/", 2)
	if len(splits) != 2 {
		return nil, fmt.Errorf("namespace missing for %q", def.Name)
	}
	namespace := splits[0]
	name := splits[1]

	return &Object{
		Name:          name,
		Namespace:     namespace,
		Relationships: relations,
		Permissions:   permissions,
	}, nil
}

func mapRelation(relation *corev1.Relation) *Relationship {
	return &Relationship{Name: relation.Name}
}

func mapPermission(relation *corev1.Relation) *Permission {
	return &Permission{
		Name: relation.Name,
	}
}
