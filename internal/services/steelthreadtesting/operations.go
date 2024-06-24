//go:build steelthread
// +build steelthread

package steelthreadtesting

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

func lookupSubjects(parameters map[string]any, client v1.PermissionsServiceClient) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	r, err := client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
		Resource: &v1.ObjectReference{
			ObjectType: parameters["resource_type"].(string),
			ObjectId:   parameters["resource_object_id"].(string),
		},
		Permission:        parameters["permission"].(string),
		SubjectObjectType: parameters["subject_type"].(string),
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	foundSubjects := mapz.NewSet[string]()
	for {
		resp, err := r.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		if !foundSubjects.Add(formatResolvedSubject(resp)) {
			return nil, errors.New("duplicate subject found")
		}
	}

	foundSubjectsSlice := foundSubjects.AsSlice()
	sort.Strings(foundSubjectsSlice)

	yamlNodes := make([]yaml.Node, 0, len(foundSubjectsSlice))
	for _, subject := range foundSubjectsSlice {
		yamlNodes = append(yamlNodes, yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: subject,
			Style: yaml.SingleQuotedStyle,
		})
	}
	return yamlNodes, nil
}

var operations = map[string]stOperation{
	"lookupSubjects": lookupSubjects,
}

func formatResolvedSubject(sub *v1.LookupSubjectsResponse) string {
	var sb strings.Builder
	sb.WriteString(sub.Subject.SubjectObjectId)

	if len(sub.ExcludedSubjects) > 0 {
		excludedSubjectStrings := make([]string, 0, len(sub.ExcludedSubjects))
		for _, excluded := range sub.ExcludedSubjects {
			excludedSubjectString := excluded.SubjectObjectId
			if excluded.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION {
				excludedSubjectString += " (conditional)"
			}

			excludedSubjectStrings = append(excludedSubjectStrings, excludedSubjectString)
		}
		sort.Strings(excludedSubjectStrings)

		sb.WriteString(" - [")
		sb.WriteString(strings.Join(excludedSubjectStrings, ", "))
		sb.WriteString("]")
	}

	if sub.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION {
		sb.WriteString(" (conditional)")
	}

	return sb.String()
}
