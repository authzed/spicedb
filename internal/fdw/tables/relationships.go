package tables

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/ccoveille/go-safecast/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"

	"github.com/authzed/spicedb/internal/fdw/common"
	"github.com/authzed/spicedb/internal/fdw/explain"
	log "github.com/authzed/spicedb/internal/logging"
)

// relationshipsField represents a field name in the relationships table
type relationshipsField string

const (
	relationshipsResourceTypeField            relationshipsField = "resource_type"
	relationshipsResourceIDField              relationshipsField = "resource_id"
	relationshipsRelationField                relationshipsField = "relation"
	relationshipsSubjectTypeField             relationshipsField = "subject_type"
	relationshipsSubjectIDField               relationshipsField = "subject_id"
	relationshipsOptionalSubjectRelationField relationshipsField = "optional_subject_relation"
	relationshipsCaveatNameField              relationshipsField = "caveat_name"
	relationshipsCaveatContextField           relationshipsField = "caveat_context"
	relationshipsConsistencyField             relationshipsField = fieldConsistency
)

var relationshipsRequiredInsertColumns = []string{
	string(relationshipsResourceTypeField),
	string(relationshipsResourceIDField),
	string(relationshipsRelationField),
	string(relationshipsSubjectTypeField),
	string(relationshipsSubjectIDField),
	string(relationshipsOptionalSubjectRelationField),
}

var relationshipsSchema = wire.Columns{
	{
		Table: 0,
		Name:  string(relationshipsResourceTypeField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(relationshipsResourceIDField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(relationshipsRelationField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(relationshipsSubjectTypeField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(relationshipsSubjectIDField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(relationshipsOptionalSubjectRelationField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(relationshipsCaveatNameField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(relationshipsCaveatContextField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	consistencyField,
}

func buildRelationshipsSelectHandler(fields fieldMap[string]) (selectHandler, error) {
	typedFields := fieldMap[relationshipsField]{}
	for k, v := range fields {
		typedFields[relationshipsField(k)] = v
	}

	filter, err := filterFromFields(typedFields, nil)
	if err != nil {
		return nil, err
	}

	consistency, err := consistencyFromFields(convertFieldMap(typedFields), nil)
	if err != nil {
		return nil, err
	}

	return readRelationshipsWithFilter(filter, consistency), nil
}

func filterFromFields(fields fieldMap[relationshipsField], _ []wire.Parameter) (*v1.RelationshipFilter, error) {
	if _, ok := fields[relationshipsCaveatNameField]; ok {
		return nil, errors.New("caveat_name is not supported in WHERE")
	}

	if _, ok := fields[relationshipsCaveatContextField]; ok {
		return nil, errors.New("caveat_context is not supported in WHERE")
	}

	if _, ok := fields[relationshipsConsistencyField]; ok {
		return nil, errors.New("consistency is not supported in WHERE")
	}

	for fieldName, fieldValue := range fields {
		if fieldValue.value == "" && fieldValue.parameterIndex == 0 && !fieldValue.isSubQueryPlaceholder {
			return nil, fmt.Errorf("field %q cannot be empty", fieldName)
		}
	}

	filter := &v1.RelationshipFilter{}
	if resourceType, ok := fields[relationshipsResourceTypeField]; ok {
		filter.ResourceType = resourceType.value
	}
	if resourceID, ok := fields[relationshipsResourceIDField]; ok {
		filter.OptionalResourceId = resourceID.value
	}
	if relation, ok := fields[relationshipsRelationField]; ok {
		filter.OptionalRelation = relation.value
	}

	if subjectType, ok := fields[relationshipsSubjectTypeField]; ok {
		if filter.OptionalSubjectFilter == nil {
			filter.OptionalSubjectFilter = &v1.SubjectFilter{}
		}
		filter.OptionalSubjectFilter.SubjectType = subjectType.value
	}

	if subjectID, ok := fields[relationshipsSubjectIDField]; ok {
		if filter.OptionalSubjectFilter == nil {
			filter.OptionalSubjectFilter = &v1.SubjectFilter{}
		}
		filter.OptionalSubjectFilter.OptionalSubjectId = subjectID.value
	}

	if subjectRelation, ok := fields[relationshipsOptionalSubjectRelationField]; ok {
		if filter.OptionalSubjectFilter == nil {
			filter.OptionalSubjectFilter = &v1.SubjectFilter{}
		}
		filter.OptionalSubjectFilter.OptionalRelation = &v1.SubjectFilter_RelationFilter{
			Relation: subjectRelation.value,
		}
	}

	return filter, nil
}

func readRelationshipsWithFilter(filter *v1.RelationshipFilter, consistency *v1.Consistency) selectHandler {
	return func(ctx context.Context, client *authzed.Client, parameters []wire.Parameter, howMany int64, writer wire.DataWriter, rowsCursor RowsCursor, selectStatement *SelectStatement) (RowsCursor, error) {
		var currentCursor *v1.Cursor
		if rowsCursor != nil {
			currentCursor = rowsCursor.(*v1.Cursor)
		}

		limit, err := safecast.Convert[uint32](howMany)
		if err != nil {
			return nil, err
		}

		stream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
			RelationshipFilter: filter,
			OptionalLimit:      limit,
			OptionalCursor:     currentCursor,
			Consistency:        consistency,
		})
		if err != nil {
			return nil, common.ConvertError(err)
		}

		rowCount := 0
		for {
			result, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return nil, common.ConvertError(err)
			}

			rowValues := make([]any, 0, len(selectStatement.columnNames))
			for _, colName := range selectStatement.columnNames {
				switch colName {
				case "resource_type":
					rowValues = append(rowValues, result.Relationship.Resource.ObjectType)
				case "resource_id":
					rowValues = append(rowValues, result.Relationship.Resource.ObjectId)
				case "relation":
					rowValues = append(rowValues, result.Relationship.Relation)
				case "subject_type":
					rowValues = append(rowValues, result.Relationship.Subject.Object.ObjectType)
				case "subject_id":
					rowValues = append(rowValues, result.Relationship.Subject.Object.ObjectId)
				case "optional_subject_relation":
					rowValues = append(rowValues, result.Relationship.Subject.OptionalRelation)
				case "caveat_name":
					if result.Relationship.OptionalCaveat == nil {
						rowValues = append(rowValues, "")
					} else {
						rowValues = append(rowValues, result.Relationship.OptionalCaveat.CaveatName)
					}
				case "caveat_context":
					if result.Relationship.OptionalCaveat == nil || result.Relationship.OptionalCaveat.Context == nil {
						rowValues = append(rowValues, "")
					} else {
						// Convert the caveat context to a JSON string.
						caveatContextMap := result.Relationship.OptionalCaveat.Context.AsMap()
						caveatContextJSON, err := json.Marshal(caveatContextMap)
						if err != nil {
							return nil, fmt.Errorf("failed to marshal caveat context: %w", err)
						}

						rowValues = append(rowValues, string(caveatContextJSON))
					}
				case "consistency":
					rowValues = append(rowValues, result.ReadAt.Token)
				default:
					return nil, fmt.Errorf("unsupported column %q", colName)
				}
			}

			if err := writer.Row(rowValues); err != nil {
				return nil, fmt.Errorf("error writing relationship row: %w", err)
			}
			currentCursor = result.AfterResultCursor
			rowCount++
		}

		return currentCursor, writer.Complete(fmt.Sprintf("SELECT %d", rowCount))
	}
}

func buildRelationshipsExplainHandler(fields fieldMap[string]) (explainHandler, error) {
	return func(ctx context.Context, client *authzed.Client, writer wire.DataWriter) error {
		cost := explain.Default("read", "relationships").String()
		log.Debug().Str("cost", cost).Msg("relationships explain cost")
		if err := writer.Row([]any{cost}); err != nil {
			return fmt.Errorf("error writing relationship explain row: %w", err)
		}
		return writer.Complete("EXPLAIN")
	}, nil
}

func relationshipsInsertRunner(ctx context.Context, client *authzed.Client, insertStmt *InsertStatement, parameters []wire.Parameter) (int64, []any, error) {
	valuesByColumnName := make(map[string]string, len(insertStmt.colNames))
	for i, colName := range insertStmt.colNames {
		// TODO: support other column types here if necessary.
		valuesByColumnName[colName] = string(parameters[i].Value())
	}

	// Ensure consistency is not specified.
	if consistency, ok := valuesByColumnName["consistency"]; ok && len(consistency) > 0 {
		return 0, nil, common.NewQueryError(errors.New("consistency column is not supported on insertion"))
	}

	rel := &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: valuesByColumnName["resource_type"],
			ObjectId:   valuesByColumnName["resource_id"],
		},
		Relation: valuesByColumnName["relation"],
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: valuesByColumnName["subject_type"],
				ObjectId:   valuesByColumnName["subject_id"],
			},
			OptionalRelation: valuesByColumnName["optional_subject_relation"],
		},
	}

	if caveatName, ok := valuesByColumnName["caveat_name"]; ok && len(caveatName) > 0 {
		rel.OptionalCaveat = &v1.ContextualizedCaveat{
			CaveatName: caveatName,
		}

		if caveatContextString, ok := valuesByColumnName["caveat_context"]; ok && len(caveatContextString) > 0 {
			// Parse the caveat context into a structpb.
			var contextMap map[string]any
			err := json.Unmarshal([]byte(caveatContextString), &contextMap)
			if err != nil {
				return 0, nil, common.NewQueryError(fmt.Errorf("invalid caveat context JSON: %w", err))
			}

			caveatContext, err := structpb.NewStruct(contextMap)
			if err != nil {
				return 0, nil, common.NewQueryError(fmt.Errorf("invalid caveat context: %w", err))
			}

			rel.OptionalCaveat.Context = caveatContext
		}
	}

	// Write the relationship.
	update := &v1.RelationshipUpdate{
		Operation:    insertStmt.metadata["writeOp"].(v1.RelationshipUpdate_Operation),
		Relationship: rel,
	}

	write, err := client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{update},
	})
	if err != nil {
		return 0, nil, common.NewQueryError(fmt.Errorf("failed to write relationship: %w", err))
	}

	values := make([]any, 0, len(insertStmt.returningColumns))
	for _, column := range insertStmt.returningColumns {
		switch column.Name {
		case "consistency":
			values = append(values, write.WrittenAt.Token)

		default:
			return 0, nil, common.NewQueryError(fmt.Errorf("unsupported column %q", column.Name))
		}
	}

	return 1, values, nil
}

func relationshipInsertValidator(ctx context.Context, query *pg_query.InsertStmt, colNames []string) (map[string]any, error) {
	// Parse the write operation.
	writeOp := v1.RelationshipUpdate_OPERATION_CREATE
	if query.GetOnConflictClause() != nil {
		if query.GetOnConflictClause().Action != pg_query.OnConflictAction_ONCONFLICT_NOTHING {
			return nil, common.NewQueryError(errors.New("only ON CONFLICT DO NOTHING is supported"))
		}

		writeOp = v1.RelationshipUpdate_OPERATION_TOUCH
	}

	return map[string]any{
		"writeOp": writeOp,
	}, nil
}

func relationshipsDeleteRunner(ctx context.Context, client *authzed.Client, deleteStmt *DeleteStatement, parameters []wire.Parameter) (int64, []any, error) {
	typedFields := fieldMap[relationshipsField]{}
	for k, v := range deleteStmt.fields {
		typedFields[relationshipsField(k)] = v
	}

	filter, err := filterFromFields(typedFields, nil)
	if err != nil {
		return 0, nil, err
	}

	// Delete the relationship(s).
	deleteResp, err := client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
		RelationshipFilter: filter,
	})
	if err != nil {
		return 0, nil, common.NewQueryError(fmt.Errorf("failed to delete relationships: %w", err))
	}

	values := make([]any, 0, len(deleteStmt.returningColumns))
	for _, column := range deleteStmt.returningColumns {
		switch column.Name {
		case "consistency":
			values = append(values, deleteResp.DeletedAt.Token)

		default:
			return 0, nil, common.NewQueryError(fmt.Errorf("unsupported column %q", column.Name))
		}
	}
	deletedCount, err := safecast.Convert[int64](deleteResp.RelationshipsDeletedCount)
	if err != nil {
		return 0, nil, common.NewQueryError(fmt.Errorf("failed to cast deleted count: %w", err))
	}
	return deletedCount, values, nil
}
