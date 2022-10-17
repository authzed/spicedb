package _missingsend

type Event struct{}

func Err(err error) *Event {
	return nil
}

func (e *Event) Send() {
}
