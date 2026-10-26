package c1zstore

import (
	"errors"
	"fmt"
)

// ID identifies a scheduled action; Revision identifies one execution of it.
type LedgerWork struct {
	SyncID            string      `json:"sync_id"`
	ID                uint64      `json:"id"`
	Revision          uint64      `json:"revision"`
	Action            LedgerChild `json:"action"`
	SchedulingKey     string      `json:"scheduling_key,omitempty"`
	TypeScopedPlanned bool        `json:"type_scoped_planned,omitempty"`
	// Revisions at which the connector invalidated this item's page token
	// and the item restarted (PageWriter.SetRestart). Unknown to SDKs that
	// predate it; their decoder drops the field.
	Invalidations []uint64 `json:"invalidations,omitempty"`
}

// Counter of restart pages committed, summed across worker buckets; the
// per-sync invalidation budget is checked against it.
const LedgerActionInvalidations = "ledger.action_invalidations"

// RestartCaps bound how often a sync may restart work: PerItem is the most
// invalidations one work item may accumulate; PerSync is the most restart
// pages the whole sync may commit. Zero is not unlimited; SetRestart
// rejects it.
type RestartCaps struct {
	PerItem uint64
	PerSync uint64
}

var ErrInvalidationCapExceeded = errors.New("action invalidation cap exceeded")

// InvalidationCapError is the refused restart: the batch did not commit and
// the pending item is unchanged. Invalidations is the item's list at the
// time of the refusal.
type InvalidationCapError struct {
	PerItem       bool
	Cap           uint64
	Observed      uint64
	WorkID        uint64
	Invalidations []uint64
}

func (e *InvalidationCapError) Error() string {
	scope := "sync"
	if e.PerItem {
		scope = "work item"
	}
	return fmt.Sprintf("%v: %s cap %d reached (%d invalidations) on work %d; restarted at revisions %v",
		ErrInvalidationCapExceeded, scope, e.Cap, e.Observed, e.WorkID, e.Invalidations)
}

func (e *InvalidationCapError) Unwrap() error { return ErrInvalidationCapExceeded }
