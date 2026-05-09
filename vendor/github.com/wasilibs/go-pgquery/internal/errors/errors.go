package errors

type Error struct {
	Message   string // exception message
	Funcname  string // source function of exception (e.g. SearchSysCache)
	Filename  string // source of exception (e.g. parse.l)
	Lineno    int    // source of exception (e.g. 104)
	Cursorpos int    // char in query at which exception occurred
	Context   string // additional context (optional, can be NULL)
}

func (e *Error) Error() string {
	return e.Message
}
