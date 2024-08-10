//go:build steelthread
// +build steelthread

package steelthreadtesting

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/protobuf/types/known/structpb"
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

func lookupResources(parameters map[string]any, client v1.PermissionsServiceClient) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var context *structpb.Struct
	if contextMap, ok := parameters["context"].(map[string]any); ok {
		c, err := structpb.NewStruct(contextMap)
		if err != nil {
			return nil, err
		}
		context = c
	}

	r, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
		ResourceObjectType: parameters["resource_type"].(string),
		Permission:         parameters["permission"].(string),
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: parameters["subject_type"].(string),
				ObjectId:   parameters["subject_object_id"].(string),
			},
		},
		Context: context,
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	foundResources := mapz.NewSet[string]()
	for {
		resp, err := r.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		if !foundResources.Add(formatResolvedResource(resp)) {
			return nil, errors.New("duplicate resource found")
		}
	}

	foundResourcesSlice := foundResources.AsSlice()
	sort.Strings(foundResourcesSlice)

	yamlNodes := make([]yaml.Node, 0, len(foundResourcesSlice))
	for _, subject := range foundResourcesSlice {
		yamlNodes = append(yamlNodes, yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: subject,
			Style: yaml.SingleQuotedStyle,
		})
	}
	return yamlNodes, nil
}

func cursoredLookupResources(parameters map[string]any, client v1.PermissionsServiceClient) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var context *structpb.Struct
	if contextMap, ok := parameters["context"].(map[string]any); ok {
		c, err := structpb.NewStruct(contextMap)
		if err != nil {
			return nil, err
		}
		context = c
	}

	var currentCursor *v1.Cursor
	nodeSets := make([][]yaml.Node, 0)
	resultCounts := make([]int, 0)
	for {
		r, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			ResourceObjectType: parameters["resource_type"].(string),
			Permission:         parameters["permission"].(string),
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: parameters["subject_type"].(string),
					ObjectId:   parameters["subject_object_id"].(string),
				},
			},
			Context: context,
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{
					FullyConsistent: true,
				},
			},
			OptionalLimit:  uint32(parameters["page_size"].(int)),
			OptionalCursor: currentCursor,
		})
		if err != nil {
			return nil, err
		}

		foundResources := mapz.NewSet[string]()
		resultCount := 0
		for {
			resp, err := r.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return nil, err
			}

			foundResources.Add(formatResolvedResource(resp))
			currentCursor = resp.AfterResultCursor
			resultCount++
		}

		if foundResources.IsEmpty() {
			break
		}

		resultCounts = append(resultCounts, resultCount)

		foundResourcesSlice := foundResources.AsSlice()
		sort.Strings(foundResourcesSlice)

		yamlNodes := make([]yaml.Node, 0, len(foundResourcesSlice))
		for _, subject := range foundResourcesSlice {
			yamlNodes = append(yamlNodes, yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: subject,
				Style: yaml.SingleQuotedStyle,
			})
		}

		nodeSets = append(nodeSets, yamlNodes)
	}

	for index, count := range resultCounts {
		if index == len(resultCounts)-1 {
			continue
		}

		if count != parameters["page_size"].(int) {
			return nil, fmt.Errorf("expected full page size of %d for page #%d (of %d), got %d\npage sizes: %v", parameters["page_size"].(int), index, len(resultCounts), count, resultCounts)
		}
	}

	return nodeSets, nil
}

var operations = map[string]stOperation{
	"lookupSubjects":          lookupSubjects,
	"lookupResources":         lookupResources,
	"cursoredLookupResources": cursoredLookupResources,
}

func formatResolvedResource(resource *v1.LookupResourcesResponse) string {
	var sb strings.Builder
	sb.WriteString(resource.ResourceObjectId)

	if resource.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION {
		sb.WriteString(" (conditional)")
	}

	return sb.String()
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
