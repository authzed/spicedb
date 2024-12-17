//go:build steelthread
// +build steelthread

package steelthreadtesting

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/tuple"
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

func bulkImportExportRelationships(parameters map[string]any, client v1.PermissionsServiceClient) (any, error) {
	// Read the list of relationships to pass to the import operation.
	importRelsFile := parameters["rels_file"].(string)

	relsFile, err := os.ReadFile("testdata/" + importRelsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read relationships file: %w", err)
	}

	// Run the import relationships.
	importRels := make([]*v1.Relationship, 0)
	for _, line := range strings.Split(string(relsFile), "\n") {
		if line == "" {
			continue
		}

		parsed, err := tuple.ParseV1Rel(line)
		if err != nil {
			return nil, err
		}

		importRels = append(importRels, parsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	r, err := client.ImportBulkRelationships(ctx)
	if err != nil {
		return nil, err
	}

	for _, rel := range importRels {
		err := r.Send(&v1.ImportBulkRelationshipsRequest{Relationships: []*v1.Relationship{rel}})
		if err != nil {
			return nil, err
		}
	}

	resp, err := r.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	if len(importRels) != int(resp.NumLoaded) {
		return nil, fmt.Errorf("expected %d relationships to be loaded, got %d", len(importRels), resp.NumLoaded)
	}

	// Run bulk export and return the results.
	var optionalLimit uint32
	if optionalLimitValue, ok := parameters["optional_limit"]; ok {
		optionalLimit = uint32(optionalLimitValue.(int))
	}

	var filter *v1.RelationshipFilter
	if resourceTypeValue, ok := parameters["resource_type"]; ok {
		filter = &v1.RelationshipFilter{
			ResourceType: resourceTypeValue.(string),
		}
	}

	if filterPrefixValue, ok := parameters["filter_resource_id_prefix"]; ok {
		filter = &v1.RelationshipFilter{
			OptionalResourceIdPrefix: filterPrefixValue.(string),
		}
	}

	exr, err := client.ExportBulkRelationships(ctx, &v1.ExportBulkRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
		OptionalLimit:              optionalLimit,
		OptionalRelationshipFilter: filter,
	})
	if err != nil {
		return nil, err
	}

	exportedRels := make([]yaml.Node, 0)
	for {
		resp, err := exr.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		for _, rel := range resp.Relationships {
			exportedRels = append(exportedRels, yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: tuple.MustV1RelString(rel),
				Style: yaml.SingleQuotedStyle,
			})
		}
	}

	return exportedRels, nil
}

func bulkCheckPermissions(parameters map[string]any, client v1.PermissionsServiceClient) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	itemStrings := parameters["check_requests"].([]string)
	checkRequests := make([]*v1.CheckBulkPermissionsRequestItem, 0, len(itemStrings))
	for _, itemString := range itemStrings {
		parsed, err := tuple.ParseV1Rel(itemString)
		if err != nil {
			return nil, err
		}

		var context *structpb.Struct
		if parsed.GetOptionalCaveat() != nil {
			context = parsed.GetOptionalCaveat().Context
		}

		checkRequests = append(checkRequests, &v1.CheckBulkPermissionsRequestItem{
			Resource:   parsed.Resource,
			Permission: parsed.Relation,
			Subject:    parsed.Subject,
			Context:    context,
		})
	}

	resp, err := client.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
		Items: checkRequests,
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	respItems := make([]yaml.Node, 0)
	for index, pair := range resp.Pairs {
		prefix := itemStrings[index] + " -> "
		if pair.GetItem() != nil {
			resultStr := pair.GetItem().Permissionship.String()
			respItems = append(respItems, yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: prefix + resultStr,
				Style: yaml.SingleQuotedStyle,
			})
		} else {
			respItems = append(respItems, yaml.Node{
				Kind:  yaml.ScalarNode,
				Value: prefix + pair.GetError().Message,
				Style: yaml.SingleQuotedStyle,
			})
		}
	}

	return respItems, nil
}

var operations = map[string]stOperation{
	"lookupSubjects":                lookupSubjects,
	"lookupResources":               lookupResources,
	"cursoredLookupResources":       cursoredLookupResources,
	"bulkImportExportRelationships": bulkImportExportRelationships,
	"bulkCheckPermissions":          bulkCheckPermissions,
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
