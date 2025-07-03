package types

// Default is the default set of types to be used in caveats, coming with all
// the standard types pre-registered. This set is frozen and cannot be modified.
var Default *StandardTypeSet

func init() {
	Default = MustNewStandardTypeSet()
	Default.Freeze()
}

// TypeSetOrDefault returns the provided TypeSet if it is not nil, otherwise it
// returns the default TypeSet. This is useful for functions that accept a
// TypeSet parameter but want to use the default if none is provided.
func TypeSetOrDefault(ts *TypeSet) *TypeSet {
	if ts == nil {
		return Default.TypeSet
	}
	return ts
}

func MustNewStandardTypeSet() *StandardTypeSet {
	sts, err := NewStandardTypeSet()
	if err != nil {
		panic(err)
	}
	return sts
}

// NewStandardTypeSet creates a new TypeSet with all the standard types pre-registered.
func NewStandardTypeSet() (*StandardTypeSet, error) {
	sts := &StandardTypeSet{
		TypeSet: NewTypeSet(),
	}

	if err := RegisterBasicTypes(sts); err != nil {
		return nil, err
	}
	return sts, nil
}

// StandardTypeSet is a TypeSet that contains all the standard types and provides nice accessors
// for each.
type StandardTypeSet struct {
	*TypeSet

	AnyType       VariableType
	BooleanType   VariableType
	StringType    VariableType
	IntType       VariableType
	UIntType      VariableType
	DoubleType    VariableType
	BytesType     VariableType
	DurationType  VariableType
	TimestampType VariableType
	IPAddressType VariableType

	listTypeBuilder GenericTypeBuilder
	mapTypeBuilder  GenericTypeBuilder
}

func (sts *StandardTypeSet) ListType(childTypes ...VariableType) (VariableType, error) {
	return sts.listTypeBuilder(childTypes...)
}

func (sts *StandardTypeSet) MapType(childTypes ...VariableType) (VariableType, error) {
	return sts.mapTypeBuilder(childTypes...)
}

func (sts *StandardTypeSet) MustListType(childTypes ...VariableType) VariableType {
	v, err := sts.ListType(childTypes...)
	if err != nil {
		panic(err)
	}
	return v
}

func (sts *StandardTypeSet) MustMapType(childTypes ...VariableType) VariableType {
	v, err := sts.MapType(childTypes...)
	if err != nil {
		panic(err)
	}
	return v
}
