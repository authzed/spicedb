package dynamodb

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

func TestAddValueToItem(t *testing.T) {
	fmt.Println("in testing")
	comp := NewCompositeFields("REL", "namespace", "subject")
	kvp := map[string]*string{
		"namespace": aws.String("user"),
		"subject":   aws.String("sub"),
	}
	// res, remaining := comp.BuildFull(kvp)

	item := Item{}

	addValueToItem(item, "pk", comp, kvp)

	fmt.Printf("%#v \n", item)
}
