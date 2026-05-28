package grant

// ErrGrantCancelled indicates a connector intentionally declined to create a grant.
type ErrGrantCancelled struct {
	Reason string
}

func (e *ErrGrantCancelled) Error() string {
	if e == nil {
		return ""
	}
	return e.Reason
}

func NewErrGrantCancelled(reason string) error {
	return &ErrGrantCancelled{Reason: reason}
}
