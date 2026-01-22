package tables

import (
	"context"
	"errors"
	"fmt"
	"io"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"

	"github.com/authzed/spicedb/internal/fdw/common"
	"github.com/authzed/spicedb/internal/fdw/stats"
	log "github.com/authzed/spicedb/internal/logging"
)

// permissionsField represents a field name in the permissions table
type permissionsField string

const (
	permissionsResourceTypeField            permissionsField = "resource_type"
	permissionsResourceIDField              permissionsField = "resource_id"
	permissionsPermissionField              permissionsField = "permission"
	permissionsSubjectTypeField             permissionsField = "subject_type"
	permissionsSubjectIDField               permissionsField = "subject_id"
	permissionsOptionalSubjectRelationField permissionsField = "optional_subject_relation"
	permissionsHasPermissionField           permissionsField = "has_permission"
)

var permissionsSchema = wire.Columns{
	{
		Table: 0,
		Name:  string(permissionsResourceTypeField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(permissionsResourceIDField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(permissionsPermissionField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(permissionsSubjectTypeField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(permissionsSubjectIDField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(permissionsOptionalSubjectRelationField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
	{
		Table: 0,
		Name:  string(permissionsHasPermissionField),
		Oid:   uint32(oid.T_bool),
		Width: 256,
	},
	consistencyField,
}

var (
	lrStats    = stats.NewPackage("permissions", "LookupResources")
	checkStats = stats.NewPackage("permissions", "CheckPermission")
	lsStats    = stats.NewPackage("permissions", "LookupSubjects")
)

func statsAndHandlerForFields(fields fieldMap[permissionsField]) (*stats.Package, func(fieldMap[permissionsField]) (selectHandler, error), error) {
	// If all fields are defined, this is a CheckPermission request.
	if fields.hasFields(permissionsResourceTypeField, permissionsResourceIDField, permissionsPermissionField, permissionsSubjectTypeField, permissionsSubjectIDField) {
		return checkStats, buildCheckPermissionHandler, nil
	}

	// If all fields except resource_id are defined, this is a LookupResources request.
	if fields.hasFields(permissionsResourceTypeField, permissionsPermissionField, permissionsSubjectTypeField, permissionsSubjectIDField) {
		return lrStats, buildLookupResourcesHandler, nil
	}

	// If all fields except subject_id are defined, this is a LookupSubjects request.
	if fields.hasFields(permissionsResourceTypeField, permissionsResourceIDField, permissionsPermissionField, permissionsSubjectTypeField) {
		return lsStats, buildLookupSubjectsHandler, nil
	}

	// Otherwise, this is unsupported.
	return nil, nil, common.NewQueryError(errors.New("unsupported query for permissions table"))
}

func buildPermissionsSelectHandler(fields fieldMap[string]) (selectHandler, error) {
	typedFields := fieldMap[permissionsField]{}
	for k, v := range fields {
		typedFields[permissionsField(k)] = v
	}

	if _, ok := typedFields[permissionsHasPermissionField]; ok {
		return nil, common.NewQueryError(errors.New("cannot select has_permission value"))
	}

	_, builder, err := statsAndHandlerForFields(typedFields)
	if err != nil {
		return nil, err
	}

	return builder(typedFields)
}

func buildCheckPermissionHandler(fields fieldMap[permissionsField]) (selectHandler, error) {
	return func(ctx context.Context, client *authzed.Client, parameters []wire.Parameter, howMany int64, writer wire.DataWriter, rowsCursor RowsCursor, selectStatement *SelectStatement) (RowsCursor, error) {
		consistency, err := consistencyFromFields(convertFieldMap(fields), parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting consistency: %w", err)
		}

		resourceType, err := stringValue(fields[permissionsResourceTypeField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting resource_type: %w", err)
		}

		resourceID, err := stringValue(fields[permissionsResourceIDField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting resource_id: %w", err)
		}

		permission, err := stringValue(fields[permissionsPermissionField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting permission: %w", err)
		}

		subjectType, err := stringValue(fields[permissionsSubjectTypeField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting subject_type: %w", err)
		}

		subjectID, err := stringValue(fields[permissionsSubjectIDField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting subject_id: %w", err)
		}

		optionalSubjectRelation, err := optionalStringValue(fields[permissionsOptionalSubjectRelationField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting optional_subject_relation: %w", err)
		}

		var trailerMD metadata.MD

		// TODO: caveat context
		checkResult, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
			Resource: &v1.ObjectReference{
				ObjectType: resourceType,
				ObjectId:   resourceID,
			},
			Permission: permission,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: subjectType,
					ObjectId:   subjectID,
				},
				OptionalRelation: optionalSubjectRelation,
			},
			Consistency: consistency,
		}, grpc.Trailer(&trailerMD))
		if err != nil {
			return nil, common.ConvertError(err)
		}

		cost, err := costFromTrailers(trailerMD)
		if err != nil {
			log.Error().Err(err).Msg("error reading cost from trailers")
		} else {
			checkStats.AddSample(cost, 1)
		}

		hasPermission := checkResult.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
		isMaybe := checkResult.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION

		rowValues, width, buildErr := buildRowValues(
			selectStatement,
			resourceType,
			resourceID,
			permission,
			subjectType,
			subjectID,
			optionalSubjectRelation,
			hasPermission,
			isMaybe,
			checkResult.CheckedAt,
		)
		if buildErr != nil {
			return nil, fmt.Errorf("error building row values: %w", buildErr)
		}
		checkStats.AddWidthSample(width)

		if err := writer.Row(rowValues); err != nil {
			return nil, fmt.Errorf("error writing check result row: %w", err)
		}
		return nil, writer.Complete("SELECT")
	}, nil
}

func buildRowValues(
	selectStatement *SelectStatement,
	resourceType string,
	resourceID string,
	permission string,
	subjectType string,
	subjectID string,
	optionalSubjectRelation string,
	hasPermission bool,
	isMaybe bool,
	zedToken *v1.ZedToken,
) ([]any, uint, error) {
	rowValues := make([]any, 0, len(selectStatement.columnNames))
	for _, colName := range selectStatement.columnNames {
		switch colName {
		case "resource_type":
			rowValues = append(rowValues, resourceType)
		case "resource_id":
			rowValues = append(rowValues, resourceID)
		case "permission":
			rowValues = append(rowValues, permission)
		case "subject_type":
			rowValues = append(rowValues, subjectType)
		case "subject_id":
			rowValues = append(rowValues, subjectID)
		case "optional_subject_relation":
			rowValues = append(rowValues, optionalSubjectRelation)
		case "has_permission":
			if isMaybe {
				rowValues = append(rowValues, nil)
			} else {
				rowValues = append(rowValues, hasPermission)
			}
		case "consistency":
			rowValues = append(rowValues, zedToken.Token)
		default:
			return nil, 0, fmt.Errorf("unsupported column %q", colName)
		}
	}

	var width uint = 1
	for _, value := range rowValues {
		if value == nil {
			continue
		}

		strValue, ok := value.(string)
		if ok {
			width += uint(len(strValue))
		}
	}

	return rowValues, width, nil
}

func buildLookupResourcesHandler(fields fieldMap[permissionsField]) (selectHandler, error) {
	return func(ctx context.Context, client *authzed.Client, parameters []wire.Parameter, howMany int64, writer wire.DataWriter, rowsCursor RowsCursor, selectStatement *SelectStatement) (RowsCursor, error) {
		consistency, err := consistencyFromFields(convertFieldMap(fields), parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting consistency: %w", err)
		}

		resourceType, err := stringValue(fields[permissionsResourceTypeField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting resource_type: %w", err)
		}

		permission, err := stringValue(fields[permissionsPermissionField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting permission: %w", err)
		}

		subjectType, err := stringValue(fields[permissionsSubjectTypeField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting subject_type: %w", err)
		}

		subjectID, err := stringValue(fields[permissionsSubjectIDField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting subject_id: %w", err)
		}

		optionalSubjectRelation, err := optionalStringValue(fields[permissionsOptionalSubjectRelationField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting optional_subject_relation: %w", err)
		}

		// TODO: support cursors
		// TODO: support caveat context
		stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			ResourceObjectType: resourceType,
			Permission:         permission,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: subjectType,
					ObjectId:   subjectID,
				},
				OptionalRelation: optionalSubjectRelation,
			},
			Consistency: consistency,
		})
		if err != nil {
			return nil, common.ConvertError(err)
		}

		for {
			result, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return nil, common.ConvertError(err)
			}

			hasPermission := result.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
			isMaybe := result.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION

			rowValues, width, buildErr := buildRowValues(
				selectStatement,
				resourceType,
				result.ResourceObjectId,
				permission,
				subjectType,
				subjectID,
				optionalSubjectRelation,
				hasPermission,
				isMaybe,
				result.LookedUpAt,
			)
			if buildErr != nil {
				return nil, fmt.Errorf("error building row values: %w", buildErr)
			}
			lrStats.AddWidthSample(width)

			if err := writer.Row(rowValues); err != nil {
				return nil, fmt.Errorf("error writing LR result row: %w", err)
			}
		}

		cost, err := costFromTrailers(stream.Trailer())
		if err != nil {
			log.Error().Err(err).Msg("error reading cost from trailers")
		} else {
			lrStats.AddSample(cost, uint(writer.Written()))
		}

		return nil, writer.Complete("SELECT")
	}, nil
}

func buildLookupSubjectsHandler(fields fieldMap[permissionsField]) (selectHandler, error) {
	return func(ctx context.Context, client *authzed.Client, parameters []wire.Parameter, howMany int64, writer wire.DataWriter, rowsCursor RowsCursor, selectStatement *SelectStatement) (RowsCursor, error) {
		consistency, err := consistencyFromFields(convertFieldMap(fields), parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting consistency: %w", err)
		}

		resourceType, err := stringValue(fields[permissionsResourceTypeField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting resource_type: %w", err)
		}

		resourceID, err := stringValue(fields[permissionsResourceIDField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting resource_id: %w", err)
		}

		permission, err := stringValue(fields[permissionsPermissionField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting permission: %w", err)
		}

		subjectType, err := stringValue(fields[permissionsSubjectTypeField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting subject_type: %w", err)
		}

		optionalSubjectRelation, err := optionalStringValue(fields[permissionsOptionalSubjectRelationField], parameters)
		if err != nil {
			return nil, fmt.Errorf("error getting optional_subject_relation: %w", err)
		}

		stream, err := client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
			Resource: &v1.ObjectReference{
				ObjectType: resourceType,
				ObjectId:   resourceID,
			},
			SubjectObjectType: subjectType,
			Permission:        permission,
			Consistency:       consistency,
		})
		if err != nil {
			return nil, common.ConvertError(err)
		}

		for {
			result, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return nil, common.ConvertError(err)
			}

			hasPermission := result.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION
			isMaybe := result.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION

			rowValues, width, buildErr := buildRowValues(
				selectStatement,
				resourceType,
				resourceID,
				permission,
				subjectType,
				result.Subject.SubjectObjectId,
				optionalSubjectRelation,
				hasPermission,
				isMaybe,
				result.LookedUpAt,
			)
			if buildErr != nil {
				return nil, fmt.Errorf("error building row values: %w", buildErr)
			}
			lsStats.AddWidthSample(width)

			if err := writer.Row(rowValues); err != nil {
				return nil, fmt.Errorf("error writing LS result row: %w", err)
			}
		}

		cost, err := costFromTrailers(stream.Trailer())
		if err != nil {
			log.Error().Err(err).Msg("error reading cost from trailers")
		} else {
			lsStats.AddSample(cost, uint(writer.Written()))
		}

		return nil, writer.Complete("SELECT")
	}, nil
}

func buildPermissionsExplainHandler(fields fieldMap[string]) (explainHandler, error) {
	typedFields := fieldMap[permissionsField]{}
	for k, v := range fields {
		typedFields[permissionsField(k)] = v
	}

	stats, _, err := statsAndHandlerForFields(typedFields)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, client *authzed.Client, writer wire.DataWriter) error {
		cost := stats.Explain()
		log.Debug().Str("cost", cost).Msg("permissions explain cost")
		if err := writer.Row([]any{cost}); err != nil {
			return fmt.Errorf("error writing permissions explain row: %w", err)
		}
		return writer.Complete("EXPLAIN")
	}, nil
}
