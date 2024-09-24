package validmarshal

type NamespaceDefinition struct{}

func (n *NamespaceDefinition) MarshalVT() (any, error) {
	return nil, nil
}

type SomeOtherObject struct{}

func (s SomeOtherObject) Marshal() {}

type NamespaceMessage struct{}

func (n *NamespaceMessage) UnmarshalVT() (*NamespaceDefinition, error) {
	return nil, nil
}

func WriteNamespaces(newConfigs ...*NamespaceDefinition) error {
	for _, newConfig := range newConfigs {
		// This is the desired usage
		_, err := newConfig.MarshalVT()
		if err != nil {
			return err
		}
	}

	return nil
}

func DoAnUnmarshal(foo *NamespaceMessage) (*NamespaceDefinition, error) {
	return foo.UnmarshalVT()
}

func DoAMarshal(foo *SomeOtherObject) {
	// Ensure that something else that isn't called proto isn't caught by the linter.
	foo.Marshal()
}
