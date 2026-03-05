package query

// CombinePlanAdvisors creates a PlanAdvisor that combines multiple advisors
// by running through them in order and returning the first non-nil result
// for either GetHints or GetMutations.
func CombinePlanAdvisors(advisors ...PlanAdvisor) PlanAdvisor {
	return &combinedAdvisor{advisors: advisors}
}

type combinedAdvisor struct {
	advisors []PlanAdvisor
}

func (c *combinedAdvisor) GetHints(outline Outline, keySource CanonicalKeySource) ([]Hint, error) {
	for _, advisor := range c.advisors {
		hints, err := advisor.GetHints(outline, keySource)
		if err != nil {
			return nil, err
		}
		if len(hints) > 0 {
			return hints, nil
		}
	}
	return nil, nil
}

func (c *combinedAdvisor) GetMutations(outline Outline, keySource CanonicalKeySource) ([]OutlineMutation, error) {
	for _, advisor := range c.advisors {
		mutations, err := advisor.GetMutations(outline, keySource)
		if err != nil {
			return nil, err
		}
		if len(mutations) > 0 {
			return mutations, nil
		}
	}
	return nil, nil
}
