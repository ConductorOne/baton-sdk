package sync

type Progress struct {
	Action         string
	ResourceTypeID string
	ResourceID     string
	Count uint32
}

func NewProgress(a *Action, c uint32) *Progress {
	p := &Progress{}
	if a == nil {
		return p
	}

	if a != nil {
		p.Count = c
		p.Action = a.Op.String()
		p.ResourceTypeID = a.ResourceTypeID
		p.ResourceID = a.ResourceID
	}

	return p
}
