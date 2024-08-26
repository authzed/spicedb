package disallowedmarshal

// NOTE: all of these defs are just to get us to `proto.Marshal` and
// `proto.Unmarshal` without needing to import things, because importing
// in these tests is difficult.
type SomeProto struct{}

func (p SomeProto) Marshal(foo any) (any, error) {
	return foo, nil
}

func (p SomeProto) Unmarshal(foo any, bar any) (any, error) {
	return foo, nil
}

type (
	NamespaceDefinition struct{}
	NamespaceMessage    struct{}
)

func WriteNamespaces(newConfigs ...*NamespaceDefinition) error {
	proto := SomeProto{}
	for _, newConfig := range newConfigs {
		_, err := proto.Marshal(newConfig) // want "use someStruct.MarshalVT instead"
		if err != nil {
			return err
		}
	}

	return nil
}

func DoAnUnmarshal() (any, error) {
	proto := SomeProto{}
	return proto.Unmarshal(make([]byte, 0), nil) // want "use someMessage.UnmarshalVT instead"
}
