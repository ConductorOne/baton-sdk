package sync

type Progress struct {
	Action               string
	ResourceTypeID       string
	ResourceID           string
	ParentResourceTypeID string
	ParentResourceID     string
	Count                uint32
}

func NewProgress(a *Action, c uint32) *Progress {
	if a == nil {
		return &Progress{}
	}

	return &Progress{
		Action:               a.Op.String(),
		ResourceTypeID:       a.ResourceTypeID,
		ResourceID:           a.ResourceID,
		ParentResourceTypeID: a.ParentResourceTypeID,
		ParentResourceID:     a.ParentResourceID,
		Count:                c,
	}
}
