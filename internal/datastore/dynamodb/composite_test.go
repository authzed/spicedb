package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

func TestNewCompositeField(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id")
	require.ElementsMatch(t, comp.fields, []string{"namespace", "object_id"})
	require.Equal(t, comp.pattern.String(), "^REL#(?<namespace>.*?)#(?<object_id>.*?)$")
}

func TestNewCompositeFieldBuild(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id")
	ns, oid := "user", "u1"
	kvp := map[string]*string{
		"namespace": &ns,
		"object_id": &oid,
	}
	res := comp.Build(kvp)

	require.Equal(t, res, "REL#user#u1")
}

func TestNewCompositeFieldBuildPartials(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id", "subject")
	ns, sub := "user", "sub"
	kvp := map[string]*string{
		"namespace": &ns,
		"subject":   &sub,
	}
	res, remaining, required := comp.BuildPartial(kvp)

	require.Equal(t, res, "REL#user#")
	require.ElementsMatch(t, remaining, []string{"subject"})
	require.ElementsMatch(t, required, []string{"subject", "object_id"})
}
func TestNewCompositeFieldBuildFull(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id", "subject")
	ns, sub, oid := "user", "sub", "u1"
	kvp := map[string]*string{
		"namespace": &ns,
		"subject":   &sub,
		"object_id": &oid,
	}
	res, remaining := comp.BuildFull(kvp)

	require.Equal(t, res, "REL#user#u1#sub")
	require.ElementsMatch(t, remaining, []string{})
}

func TestNewCompositeFieldBuildFullRemainingFields(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id", "subject")
	ns, sub, oid := "user", "sub", "u1"
	kvp := map[string]*string{
		"namespace": &ns,
		"subject":   &sub,
		"object_id": &oid,
		"relation":  aws.String("writer"),
	}
	res, remaining := comp.BuildFull(kvp)

	require.Equal(t, res, "REL#user#u1#sub")
	require.ElementsMatch(t, remaining, []string{"relation"})
}

func TestNewCompositeFieldBuildFullMissingOrder(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id", "subject")
	ns, sub := "user", "sub"
	kvp := map[string]*string{
		"namespace": &ns,
		"subject":   &sub,
		"relation":  aws.String("writer"),
	}
	res, remaining := comp.BuildFull(kvp)

	require.Equal(t, res, "")
	require.ElementsMatch(t, remaining, []string{"relation", "namespace", "subject"})
}

func TestNewCompositeFieldCheck(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id", "subject")
	availables := []string{
		"namespace",
		"subject",
	}
	unused, required := comp.Check(availables)

	require.ElementsMatch(t, unused, []string{"subject"})
	require.ElementsMatch(t, required, []string{"object_id", "subject"})
}

func TestNewCompositeFieldExtract(t *testing.T) {
	comp := NewCompositeFields("REL", "namespace", "object_id", "relation")

	var ns, rel, obj string

	kvp := map[string]*string{
		"namespace": &ns,
		"relation":  &rel,
		"object_id": &obj,
	}

	comp.Extract("REL#user#u1#writer", kvp)

	require.Equal(t, ns, "user")
	require.Equal(t, obj, "u1")
	require.Equal(t, rel, "writer")
}
