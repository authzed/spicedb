package dynamodb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCompositeField(t *testing.T) {
	comp := NewCompositeFields("REL#${namespace}#OBJ#${object_id}")
	require.ElementsMatch(t, comp.fields, []string{"namespace", "object_id"})
	require.Equal(t, comp.pattern.String(), "REL#(?<namespace>.*?)#OBJ#(?<object_id>.*?)")
}

func TestNewCompositeFieldBuild(t *testing.T) {
	comp := NewCompositeFields("REL#${namespace}#OBJ#${object_id}")
	ns, oid := "user", "u1"
	kvp := map[string]*string{
		"namespace": &ns,
		"object_id": &oid,
	}
	res := comp.Build(kvp)

	require.Equal(t, res, "REL#user#OBJ#u1")
}

func TestNewCompositeFieldBuildPartials(t *testing.T) {
	comp := NewCompositeFields("REL#${namespace}#OBJ#${object_id}#${subject}")
	ns, sub := "user", "sub"
	kvp := map[string]*string{
		"namespace": &ns,
		"subject":   &sub,
	}
	res, remaining := comp.BuildPartial(kvp)

	fmt.Printf("%#v", remaining)

	require.Equal(t, res, "REL#user#OBJ#")
	require.ElementsMatch(t, remaining, []string{"subject"})
}

func TestNewCompositeFieldExtract(t *testing.T) {
	comp := NewCompositeFields("REL#${namespace}#OBJ#${object_id}#${relation}")

	var ns, rel, obj string

	kvp := map[string]*string{
		"namespace": &ns,
		"relation":  &rel,
		"object_id": &obj,
	}

	comp.Extract("REL#user#OBJ#u1#writer", kvp)

	require.Equal(t, ns, "user")
	require.Equal(t, obj, "u1")
	require.Equal(t, rel, "writer")

}
