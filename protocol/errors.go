package protocol

type RecoverableError struct {
	errMsg string
}

func (e *RecoverableError) Error() string {
	return e.errMsg
}
