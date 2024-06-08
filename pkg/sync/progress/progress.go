package progress

import "github.com/conductorone/baton-sdk/pkg/sync/action"

type Progress struct {
	Action               string
	ResourceTypeID       string
	ResourceID           string
	ParentResourceTypeID string
	ParentResourceID     string
	Count                uint32
}

func New(a *action.Action, c uint32) *Progress {
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
