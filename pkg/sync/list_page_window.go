package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"

// ListPageWindowReport describes a list walk that reached the configured window.
// VendorTerminated is false when a nonempty continuation was cleared (sampled),
// and true when the vendor ended the list at or after the window (complete).
type ListPageWindowReport struct {
	Op                   ActionOp
	ResourceTypeID       string
	ResourceID           string
	ParentResourceTypeID string
	ParentResourceID     string
	TypeScoped           bool
	Spawned              bool
	PagesReturned        uint32
	Window               uint32
	VendorTerminated     bool
}

func (s *syncer) applyListPageWindow(action *Action, next string) (string, *ListPageWindowReport) {
	window := s.cfg.listPageWindow
	if window == 0 || action.ResourceTypeID == "" {
		return next, nil
	}
	switch action.Op {
	case SyncResourcesOp, SyncEntitlementsOp, SyncGrantsOp:
	default:
		return next, nil
	}
	if action.WindowPages < window {
		action.WindowPages++
	}
	if action.WindowPages < window {
		return next, nil
	}
	report := &ListPageWindowReport{
		Op:                   action.Op,
		ResourceTypeID:       action.ResourceTypeID,
		ResourceID:           action.ResourceID,
		ParentResourceTypeID: action.ParentResourceTypeID,
		ParentResourceID:     action.ParentResourceID,
		TypeScoped:           action.TypeScoped,
		Spawned:              action.Spawned,
		PagesReturned:        action.WindowPages,
		Window:               window,
		VendorTerminated:     next == "",
	}
	return "", report
}

func (s *syncer) reportListPageWindow(report ListPageWindowReport) {
	if s.cfg.listPageWindowHandler != nil {
		s.cfg.listPageWindowHandler(report)
	}
}

func (s *syncer) stageListPageWindow(page *ledgerPage, action *Action) {
	next, report := s.applyListPageWindow(action, page.row.NextPageToken)
	page.row.NextPageToken = next
	if report == nil {
		return
	}
	if !report.VendorTerminated {
		if page.observations.Counters == nil {
			page.observations.Counters = make(map[string]uint64)
		}
		page.observations.Counters[c1zstore.ListPageWindowCounterPrefix+"sampled"]++
		page.observations.Counters[c1zstore.ListPageWindowCounterPrefix+"sampled:"+report.ResourceTypeID]++
	}
	page.afterCommit = append(page.afterCommit, func() { s.reportListPageWindow(*report) })
}
