package disallowedmarshal

type WithError interface {
	Err(error) WithError
	Something()
}

type someError struct {
	error
}

func (err someError) MarshalZerologObject(e WithError) {
	e.Err(err) // want "will cause infinite recursion"
}

type someError2 struct {
	error
}

func (err someError2) MarshalZerologObject(e WithError) {
	e.Err(err).Something() // want "will cause infinite recursion"
}
