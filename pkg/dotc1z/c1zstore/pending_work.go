package c1zstore

// ID identifies a scheduled action; Revision identifies one execution of it.
// Equal request arguments do not identify equal work.
type LedgerWork struct {
	SyncID            string      `json:"sync_id"`
	ID                uint64      `json:"id"`
	Revision          uint64      `json:"revision"`
	Action            LedgerChild `json:"action"`
	SchedulingKey     string      `json:"scheduling_key,omitempty"`
	TypeScopedPlanned bool        `json:"type_scoped_planned,omitempty"`
}
