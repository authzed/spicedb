package validmarshal

type someError struct {
	error
}

type WithError interface {
	Err(error)
}

func (err someError) MarshalZerologObject(e WithError) {
	e.Err(err.error)
}
