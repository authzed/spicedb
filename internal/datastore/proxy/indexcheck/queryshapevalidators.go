package indexcheck

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func validateCaveatFilter(filter datastore.RelationshipsFilter, queryShapeName string) error {
	if filter.OptionalCaveatNameFilter.Option != datastore.CaveatFilterOptionNone {
		return fmt.Errorf("optional caveats not supported for %s", queryShapeName)
	}
	return nil
}

func validateResourceType(resourceType, queryShapeName string, required bool) error {
	if required && resourceType == "" {
		return fmt.Errorf("optional resource type required for %s", queryShapeName)
	}
	if !required && resourceType != "" {
		return fmt.Errorf("no optional resource type allowed for %s", queryShapeName)
	}
	return nil
}

func validateResourceIDs(resourceIds []string, queryShapeName string, required bool) error {
	if required && len(resourceIds) == 0 {
		return fmt.Errorf("optional resource ids required for %s", queryShapeName)
	}
	if !required && len(resourceIds) != 0 {
		return fmt.Errorf("no optional resource ids allowed for %s", queryShapeName)
	}
	return nil
}

func validateResourceIDPrefix(resourceIDPrefix string, queryShapeName string) error {
	if len(resourceIDPrefix) != 0 {
		return fmt.Errorf("no optional resource id prefixes allowed for %s", queryShapeName)
	}
	return nil
}

func validateResourceRelation(resourceRelation, queryShapeName string, required bool) error {
	if required && resourceRelation == "" {
		return fmt.Errorf("optional resource relation required for %s", queryShapeName)
	}
	if !required && resourceRelation != "" {
		return fmt.Errorf("no optional resource relation allowed for %s", queryShapeName)
	}
	return nil
}

func validateSubjectsSelectors(selectors []datastore.SubjectsSelector, queryShapeName string, required bool) error {
	if required && len(selectors) == 0 {
		return fmt.Errorf("optional subjects selectors required for %s", queryShapeName)
	}
	if !required && len(selectors) != 0 {
		return fmt.Errorf("no optional subjects selectors allowed for %s", queryShapeName)
	}
	return nil
}

func validateDirectSubjectsSelectors(selectors []datastore.SubjectsSelector, queryShapeName string) error {
	for _, subjectSelector := range selectors {
		if subjectSelector.OptionalSubjectType == "" {
			return fmt.Errorf("optional subject type required for %s", queryShapeName)
		}
		if len(subjectSelector.OptionalSubjectIds) == 0 {
			return fmt.Errorf("optional subject ids required for %s", queryShapeName)
		}
	}
	return nil
}

func validateIndirectSubjectsSelectors(selectors []datastore.SubjectsSelector, queryShapeName string) error {
	for _, subjectSelector := range selectors {
		if subjectSelector.OptionalSubjectType != "" {
			return fmt.Errorf("optional subject type required for %s", queryShapeName)
		}
		if len(subjectSelector.OptionalSubjectIds) != 0 {
			return fmt.Errorf("no optional subject ids allowed for %s", queryShapeName)
		}
		if subjectSelector.RelationFilter.IsEmpty() {
			return fmt.Errorf("relation filter required for %s", queryShapeName)
		}
		if !subjectSelector.RelationFilter.OnlyNonEllipsisRelations {
			return fmt.Errorf("only non-ellipsis relations allowed for %s", queryShapeName)
		}
	}
	return nil
}

func validateSubjectType(subjectType, queryShapeName string) error {
	if subjectType == "" {
		return fmt.Errorf("subject type required for %s", queryShapeName)
	}
	return nil
}

func validateSubjectIDs(subjectIds []string, queryShapeName string, required bool) error {
	if required && len(subjectIds) == 0 {
		return fmt.Errorf("subject ids required for %s", queryShapeName)
	}
	if !required && len(subjectIds) != 0 {
		return fmt.Errorf("no optional subject ids allowed for %s", queryShapeName)
	}
	return nil
}

func validateSubjectRelation(subjectRelation datastore.SubjectRelationFilter, queryShapeName string, required bool) error {
	if required && subjectRelation.IsEmpty() {
		return fmt.Errorf("subject relation required for %s", queryShapeName)
	}
	if !required && !subjectRelation.IsEmpty() {
		return fmt.Errorf("no optional subject relation allowed for %s", queryShapeName)
	}
	return nil
}

func validateResRelation(resRelation *options.ResourceRelation, queryShapeName string) error {
	if resRelation == nil {
		return fmt.Errorf("resource relation required for %s", queryShapeName)
	}
	if resRelation.Namespace == "" {
		return fmt.Errorf("resource relation namespace required for %s", queryShapeName)
	}
	if resRelation.Relation == "" {
		return fmt.Errorf("resource relation required for %s", queryShapeName)
	}
	return nil
}

func validateCheckPermissionSelectCommon(filter datastore.RelationshipsFilter, queryShapeName string) error {
	if err := validateCaveatFilter(filter, queryShapeName); err != nil {
		return err
	}
	if err := validateResourceType(filter.OptionalResourceType, queryShapeName, true); err != nil {
		return err
	}
	if err := validateResourceIDs(filter.OptionalResourceIds, queryShapeName, true); err != nil {
		return err
	}
	if err := validateResourceIDPrefix(filter.OptionalResourceIDPrefix, queryShapeName); err != nil {
		return err
	}
	if err := validateResourceRelation(filter.OptionalResourceRelation, queryShapeName, true); err != nil {
		return err
	}
	if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShapeName, true); err != nil {
		return err
	}
	return nil
}

func validateResourceTypeAndSubjectsCommon(filter datastore.RelationshipsFilter, queryShapeName string) error {
	if err := validateResourceType(filter.OptionalResourceType, queryShapeName, true); err != nil {
		return err
	}
	if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShapeName, false); err != nil {
		return err
	}
	return nil
}

func validateQueryShape(queryShape queryshape.Shape, filter datastore.RelationshipsFilter) error {
	queryShapeName := string(queryShape)

	switch queryShape {
	case queryshape.CheckPermissionSelectDirectSubjects:
		if err := validateCheckPermissionSelectCommon(filter, queryShapeName); err != nil {
			return err
		}
		if err := validateDirectSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShapeName); err != nil {
			return err
		}
		return nil

	case queryshape.CheckPermissionSelectIndirectSubjects:
		if err := validateCheckPermissionSelectCommon(filter, queryShapeName); err != nil {
			return err
		}
		if err := validateIndirectSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShapeName); err != nil {
			return err
		}
		return nil

	case queryshape.AllSubjectsForResources:
		if err := validateCaveatFilter(filter, queryShapeName); err != nil {
			return err
		}
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShapeName, true); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShapeName, true); err != nil {
			return err
		}
		if err := validateResourceTypeAndSubjectsCommon(filter, queryShapeName); err != nil {
			return err
		}
		return nil

	case queryshape.FindResourceOfType:
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShapeName, false); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShapeName, false); err != nil {
			return err
		}
		if err := validateResourceTypeAndSubjectsCommon(filter, queryShapeName); err != nil {
			return err
		}
		return nil

	case queryshape.FindResourceAndSubjectWithRelations:
		if err := validateResourceType(filter.OptionalResourceType, queryShapeName, true); err != nil {
			return err
		}
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShapeName, false); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShapeName, true); err != nil {
			return err
		}
		if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShapeName, true); err != nil {
			return err
		}
		if len(filter.OptionalSubjectsSelectors) != 1 {
			return fmt.Errorf("exactly one subjects selector required for %s", queryShapeName)
		}
		if err := validateSubjectType(filter.OptionalSubjectsSelectors[0].OptionalSubjectType, queryShapeName); err != nil {
			return err
		}
		if err := validateSubjectIDs(filter.OptionalSubjectsSelectors[0].OptionalSubjectIds, queryShapeName, false); err != nil {
			return err
		}
		if err := validateSubjectRelation(filter.OptionalSubjectsSelectors[0].RelationFilter, queryShapeName, true); err != nil {
			return err
		}
		return nil

	case queryshape.FindSubjectOfTypeAndRelation:
		if err := validateResourceType(filter.OptionalResourceType, queryShapeName, false); err != nil {
			return err
		}
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShapeName, false); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShapeName, false); err != nil {
			return err
		}
		if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShapeName, true); err != nil {
			return err
		}
		if len(filter.OptionalSubjectsSelectors) != 1 {
			return fmt.Errorf("exactly one subjects selector required for %s", queryShapeName)
		}
		if err := validateSubjectType(filter.OptionalSubjectsSelectors[0].OptionalSubjectType, queryShapeName); err != nil {
			return err
		}
		if err := validateSubjectIDs(filter.OptionalSubjectsSelectors[0].OptionalSubjectIds, queryShapeName, false); err != nil {
			return err
		}
		if err := validateSubjectRelation(filter.OptionalSubjectsSelectors[0].RelationFilter, queryShapeName, true); err != nil {
			return err
		}
		return nil

	case queryshape.FindResourceRelationForSubjectRelation:
		if err := validateResourceType(filter.OptionalResourceType, queryShapeName, true); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShapeName, true); err != nil {
			return err
		}
		if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShapeName, true); err != nil {
			return err
		}
		if len(filter.OptionalSubjectsSelectors) != 1 {
			return fmt.Errorf("exactly one subjects selector required for %s", queryShapeName)
		}
		if err := validateSubjectType(filter.OptionalSubjectsSelectors[0].OptionalSubjectType, queryShapeName); err != nil {
			return err
		}
		if err := validateSubjectRelation(filter.OptionalSubjectsSelectors[0].RelationFilter, queryShapeName, true); err != nil {
			return err
		}
		return nil

	case queryshape.Varying:
		// Nothing to validate.
		return nil

	case queryshape.Unspecified:
		fallthrough

	case "":
		return spiceerrors.MustBugf("query shape must be specified")

	default:
		return fmt.Errorf("unsupported query shape: %s", queryShape)
	}
}

func validateReverseQueryShape(queryShape queryshape.Shape, subjectFilter datastore.SubjectsFilter, queryOpts *options.ReverseQueryOptions) error {
	queryShapeName := string(queryShape)

	switch queryShape {
	case queryshape.MatchingResourcesForSubject:
		if err := validateSubjectType(subjectFilter.SubjectType, queryShapeName); err != nil {
			return err
		}
		if err := validateSubjectIDs(subjectFilter.OptionalSubjectIds, queryShapeName, true); err != nil {
			return err
		}
		if err := validateResRelation(queryOpts.ResRelation, queryShapeName); err != nil {
			return err
		}
		return nil

	case queryshape.Varying:
		// Nothing to validate.
		return nil

	case queryshape.Unspecified:
		fallthrough

	case "":
		return spiceerrors.MustBugf("query shape must be specified")

	default:
		return fmt.Errorf("unsupported query shape: %s", queryShape)
	}
}
