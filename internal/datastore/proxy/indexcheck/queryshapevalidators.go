package indexcheck

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func validateQueryShape(queryShape queryshape.Shape, filter datastore.RelationshipsFilter) error {
	switch queryShape {
	case queryshape.CheckPermissionSelectDirectSubjects:
		if filter.OptionalCaveatNameFilter.Option != datastore.CaveatFilterOptionNone {
			return fmt.Errorf("optional caveats not supported for CheckPermissionSelectDirectSubjects")
		}

		if filter.OptionalResourceType == "" {
			return fmt.Errorf("optional resource type required for CheckPermissionSelectDirectSubjects")
		}

		if len(filter.OptionalResourceIds) == 0 {
			return fmt.Errorf("optional resource ids required for CheckPermissionSelectDirectSubjects")
		}

		if len(filter.OptionalResourceIDPrefix) != 0 {
			return fmt.Errorf("no optional resource id prefixes allowed for CheckPermissionSelectDirectSubjects")
		}

		if filter.OptionalResourceRelation == "" {
			return fmt.Errorf("optional resource relation required for CheckPermissionSelectDirectSubjects")
		}

		if len(filter.OptionalSubjectsSelectors) == 0 {
			return fmt.Errorf("optional subjects selectors required for CheckPermissionSelectDirectSubjects")
		}

		for _, subjectSelector := range filter.OptionalSubjectsSelectors {
			if subjectSelector.OptionalSubjectType == "" {
				return fmt.Errorf("optional subject type required for CheckPermissionSelectDirectSubjects")
			}

			if len(subjectSelector.OptionalSubjectIds) == 0 {
				return fmt.Errorf("optional subject ids required for CheckPermissionSelectDirectSubjects")
			}
		}

		return nil

	case queryshape.CheckPermissionSelectIndirectSubjects:
		if filter.OptionalCaveatNameFilter.Option != datastore.CaveatFilterOptionNone {
			return fmt.Errorf("optional caveats not supported for CheckPermissionSelectIndirectSubjects")
		}

		if filter.OptionalResourceType == "" {
			return fmt.Errorf("optional resource type required for CheckPermissionSelectIndirectSubjects")
		}

		if len(filter.OptionalResourceIDPrefix) != 0 {
			return fmt.Errorf("no optional resource id prefixes allowed for CheckPermissionSelectDirectSubjects")
		}

		if len(filter.OptionalResourceIds) == 0 {
			return fmt.Errorf("optional resource ids required for CheckPermissionSelectIndirectSubjects")
		}

		if filter.OptionalResourceRelation == "" {
			return fmt.Errorf("optional resource relation required for CheckPermissionSelectIndirectSubjects")
		}

		if len(filter.OptionalSubjectsSelectors) == 0 {
			return fmt.Errorf("optional subjects selectors required for CheckPermissionSelectIndirectSubjects")
		}

		for _, subjectSelector := range filter.OptionalSubjectsSelectors {
			if subjectSelector.OptionalSubjectType != "" {
				return fmt.Errorf("optional subject type required for CheckPermissionSelectIndirectSubjects")
			}

			if len(subjectSelector.OptionalSubjectIds) != 0 {
				return fmt.Errorf("no optional subject ids allowed for CheckPermissionSelectIndirectSubjects")
			}

			if subjectSelector.RelationFilter.IsEmpty() {
				return fmt.Errorf("relation filter required for CheckPermissionSelectIndirectSubjects")
			}

			if !subjectSelector.RelationFilter.OnlyNonEllipsisRelations {
				return fmt.Errorf("only non-ellipsis relations allowed for CheckPermissionSelectIndirectSubjects")
			}
		}

		return nil

	case queryshape.AllSubjectsForResources:
		if filter.OptionalCaveatNameFilter.Option != datastore.CaveatFilterOptionNone {
			return fmt.Errorf("optional caveats not supported for AllSubjectsForResources")
		}

		if filter.OptionalResourceType == "" {
			return fmt.Errorf("optional resource type required for AllSubjectsForResources")
		}

		if len(filter.OptionalResourceIds) == 0 {
			return fmt.Errorf("optional resource ids required for AllSubjectsForResources")
		}

		if filter.OptionalResourceRelation == "" {
			return fmt.Errorf("optional resource relation required for AllSubjectsForResources")
		}

		if len(filter.OptionalSubjectsSelectors) != 0 {
			return fmt.Errorf("no optional subjects selectors allowed for AllSubjectsForResources")
		}

		return nil

	case queryshape.FindResourceOfType:
		if filter.OptionalResourceType == "" {
			return fmt.Errorf("optional resource type required for FindResourceOfType")
		}

		if len(filter.OptionalResourceIds) != 0 {
			return fmt.Errorf("no optional resource ids allowed for FindResourceOfType")
		}

		if filter.OptionalResourceRelation != "" {
			return fmt.Errorf("no optional resource relation allowed for FindResourceOfType")
		}

		if len(filter.OptionalSubjectsSelectors) != 0 {
			return fmt.Errorf("no optional subjects selectors allowed for FindResourceOfType")
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
		if subjectFilter.SubjectType == "" {
			return fmt.Errorf("subject type required for MatchingResourcesForSubject")
		}

		if len(subjectFilter.OptionalSubjectIds) == 0 {
			return fmt.Errorf("subject ids required for MatchingResourcesForSubject")
		}

		if queryOpts.ResRelation == nil {
			return fmt.Errorf("resource relation required for MatchingResourcesForSubject")
		}

		if queryOpts.ResRelation.Namespace == "" {
			return fmt.Errorf("resource relation namespace required for MatchingResourcesForSubject")
		}

		if queryOpts.ResRelation.Relation == "" {
			return fmt.Errorf("resource relation required for MatchingResourcesForSubject")
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
