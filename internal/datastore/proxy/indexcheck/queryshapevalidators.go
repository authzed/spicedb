package indexcheck

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func validateCaveatFilter(filter datastore.RelationshipsFilter, queryShape queryshape.Shape) error {
	if filter.OptionalCaveatNameFilter.Option != datastore.CaveatFilterOptionNone {
		return fmt.Errorf("optional caveats not supported for %s", queryShape)
	}
	return nil
}

func validateResourceType(resourceType string, queryShape queryshape.Shape, required bool) error {
	if required && resourceType == "" {
		return fmt.Errorf("optional resource type required for %s", queryShape)
	}
	if !required && resourceType != "" {
		return fmt.Errorf("no optional resource type allowed for %s", queryShape)
	}
	return nil
}

func validateResourceIDs(resourceIds []string, queryShape queryshape.Shape, required bool) error {
	if required && len(resourceIds) == 0 {
		return fmt.Errorf("optional resource ids required for %s", queryShape)
	}
	if !required && len(resourceIds) != 0 {
		return fmt.Errorf("no optional resource ids allowed for %s", queryShape)
	}
	return nil
}

func validateResourceIDPrefix(resourceIDPrefix string, queryShape queryshape.Shape) error {
	if len(resourceIDPrefix) != 0 {
		return fmt.Errorf("no optional resource id prefixes allowed for %s", queryShape)
	}
	return nil
}

func validateResourceRelation(resourceRelation string, queryShape queryshape.Shape, required bool) error {
	if required && resourceRelation == "" {
		return fmt.Errorf("optional resource relation required for %s", queryShape)
	}
	if !required && resourceRelation != "" {
		return fmt.Errorf("no optional resource relation allowed for %s", queryShape)
	}
	return nil
}

func validateSubjectsSelectors(selectors []datastore.SubjectsSelector, queryShape queryshape.Shape, required bool) error {
	if required && len(selectors) == 0 {
		return fmt.Errorf("optional subjects selectors required for %s", queryShape)
	}
	if !required && len(selectors) != 0 {
		return fmt.Errorf("no optional subjects selectors allowed for %s", queryShape)
	}
	return nil
}

func validateDirectSubjectsSelectors(selectors []datastore.SubjectsSelector, queryShape queryshape.Shape) error {
	for _, subjectSelector := range selectors {
		if subjectSelector.OptionalSubjectType == "" {
			return fmt.Errorf("optional subject type required for %s", queryShape)
		}
		if len(subjectSelector.OptionalSubjectIds) == 0 {
			return fmt.Errorf("optional subject ids required for %s", queryShape)
		}
	}
	return nil
}

func validateIndirectSubjectsSelectors(selectors []datastore.SubjectsSelector, queryShape queryshape.Shape) error {
	for _, subjectSelector := range selectors {
		if subjectSelector.OptionalSubjectType != "" {
			return fmt.Errorf("no optional subject type allowed for %s", queryShape)
		}
		if len(subjectSelector.OptionalSubjectIds) != 0 {
			return fmt.Errorf("no optional subject ids allowed for %s", queryShape)
		}
		if subjectSelector.RelationFilter.IsEmpty() {
			return fmt.Errorf("relation filter required for %s", queryShape)
		}
		if !subjectSelector.RelationFilter.OnlyNonEllipsisRelations {
			return fmt.Errorf("only non-ellipsis relations allowed for %s", queryShape)
		}
	}
	return nil
}

func validateSubjectType(subjectType string, queryShape queryshape.Shape) error {
	if subjectType == "" {
		return fmt.Errorf("subject type required for %s", queryShape)
	}
	return nil
}

func validateSubjectIDs(subjectIds []string, queryShape queryshape.Shape, required bool) error {
	if required && len(subjectIds) == 0 {
		return fmt.Errorf("subject ids required for %s", queryShape)
	}
	if !required && len(subjectIds) != 0 {
		return fmt.Errorf("no optional subject ids allowed for %s", queryShape)
	}
	return nil
}

func validateSubjectRelation(subjectRelation datastore.SubjectRelationFilter, queryShape queryshape.Shape, required bool) error {
	if required && subjectRelation.IsEmpty() {
		return fmt.Errorf("subject relation required for %s", queryShape)
	}
	if !required && !subjectRelation.IsEmpty() {
		return fmt.Errorf("no optional subject relation allowed for %s", queryShape)
	}
	return nil
}

func validateResRelation(resRelation *options.ResourceRelation, queryShape queryshape.Shape) error {
	if resRelation == nil {
		return fmt.Errorf("resource relation required for %s", queryShape)
	}
	if resRelation.Namespace == "" {
		return fmt.Errorf("resource relation namespace required for %s", queryShape)
	}
	if resRelation.Relation == "" {
		return fmt.Errorf("resource relation required for %s", queryShape)
	}
	return nil
}

func validateCheckPermissionSelectCommon(filter datastore.RelationshipsFilter, queryShape queryshape.Shape) error {
	if err := validateCaveatFilter(filter, queryShape); err != nil {
		return err
	}
	if err := validateResourceType(filter.OptionalResourceType, queryShape, true); err != nil {
		return err
	}
	if err := validateResourceIDs(filter.OptionalResourceIds, queryShape, true); err != nil {
		return err
	}
	if err := validateResourceIDPrefix(filter.OptionalResourceIDPrefix, queryShape); err != nil {
		return err
	}
	if err := validateResourceRelation(filter.OptionalResourceRelation, queryShape, true); err != nil {
		return err
	}
	if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShape, true); err != nil {
		return err
	}
	return nil
}

func validateResourceTypeAndSubjectsCommon(filter datastore.RelationshipsFilter, queryShape queryshape.Shape) error {
	if err := validateResourceType(filter.OptionalResourceType, queryShape, true); err != nil {
		return err
	}
	if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShape, false); err != nil {
		return err
	}
	return nil
}

func validateQueryShape(queryShape queryshape.Shape, filter datastore.RelationshipsFilter) error {
	switch queryShape {
	case queryshape.CheckPermissionSelectDirectSubjects:
		if err := validateCheckPermissionSelectCommon(filter, queryShape); err != nil {
			return err
		}
		if err := validateDirectSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShape); err != nil {
			return err
		}
		return nil

	case queryshape.CheckPermissionSelectIndirectSubjects:
		if err := validateCheckPermissionSelectCommon(filter, queryShape); err != nil {
			return err
		}
		if err := validateIndirectSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShape); err != nil {
			return err
		}
		return nil

	case queryshape.AllSubjectsForResources:
		if err := validateCaveatFilter(filter, queryShape); err != nil {
			return err
		}
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShape, true); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShape, true); err != nil {
			return err
		}
		if err := validateResourceTypeAndSubjectsCommon(filter, queryShape); err != nil {
			return err
		}
		return nil

	case queryshape.FindResourceOfType:
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShape, false); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShape, false); err != nil {
			return err
		}
		if err := validateResourceTypeAndSubjectsCommon(filter, queryShape); err != nil {
			return err
		}
		return nil

	case queryshape.FindResourceAndSubjectWithRelations:
		if err := validateResourceType(filter.OptionalResourceType, queryShape, true); err != nil {
			return err
		}
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShape, false); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShape, true); err != nil {
			return err
		}
		if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShape, true); err != nil {
			return err
		}
		if len(filter.OptionalSubjectsSelectors) != 1 {
			return fmt.Errorf("exactly one subjects selector required for %s", queryShape)
		}
		if err := validateSubjectType(filter.OptionalSubjectsSelectors[0].OptionalSubjectType, queryShape); err != nil {
			return err
		}
		if err := validateSubjectIDs(filter.OptionalSubjectsSelectors[0].OptionalSubjectIds, queryShape, false); err != nil {
			return err
		}
		if err := validateSubjectRelation(filter.OptionalSubjectsSelectors[0].RelationFilter, queryShape, true); err != nil {
			return err
		}
		return nil

	case queryshape.FindSubjectOfTypeAndRelation:
		if err := validateResourceType(filter.OptionalResourceType, queryShape, false); err != nil {
			return err
		}
		if err := validateResourceIDs(filter.OptionalResourceIds, queryShape, false); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShape, false); err != nil {
			return err
		}
		if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShape, true); err != nil {
			return err
		}
		if len(filter.OptionalSubjectsSelectors) != 1 {
			return fmt.Errorf("exactly one subjects selector required for %s", queryShape)
		}
		if err := validateSubjectType(filter.OptionalSubjectsSelectors[0].OptionalSubjectType, queryShape); err != nil {
			return err
		}
		if err := validateSubjectIDs(filter.OptionalSubjectsSelectors[0].OptionalSubjectIds, queryShape, false); err != nil {
			return err
		}
		if err := validateSubjectRelation(filter.OptionalSubjectsSelectors[0].RelationFilter, queryShape, true); err != nil {
			return err
		}
		return nil

	case queryshape.FindResourceRelationForSubjectRelation:
		if err := validateResourceType(filter.OptionalResourceType, queryShape, true); err != nil {
			return err
		}
		if err := validateResourceRelation(filter.OptionalResourceRelation, queryShape, true); err != nil {
			return err
		}
		if err := validateSubjectsSelectors(filter.OptionalSubjectsSelectors, queryShape, true); err != nil {
			return err
		}
		if len(filter.OptionalSubjectsSelectors) != 1 {
			return fmt.Errorf("exactly one subjects selector required for %s", queryShape)
		}
		if err := validateSubjectType(filter.OptionalSubjectsSelectors[0].OptionalSubjectType, queryShape); err != nil {
			return err
		}
		if err := validateSubjectRelation(filter.OptionalSubjectsSelectors[0].RelationFilter, queryShape, true); err != nil {
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
	switch queryShape {
	case queryshape.MatchingResourcesForSubject:
		if err := validateSubjectType(subjectFilter.SubjectType, queryShape); err != nil {
			return err
		}
		if err := validateSubjectIDs(subjectFilter.OptionalSubjectIds, queryShape, true); err != nil {
			return err
		}
		if err := validateResRelation(queryOpts.ResRelation, queryShape); err != nil {
			return err
		}
		return nil

	case queryshape.FindSubjectOfTypeAndRelation:
		if err := validateSubjectType(subjectFilter.SubjectType, queryShape); err != nil {
			return err
		}
		if err := validateSubjectRelation(subjectFilter.RelationFilter, queryShape, true); err != nil {
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
