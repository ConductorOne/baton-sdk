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
	// Revisions at which the connector asked for this item to restart
	// (PageWriter.SetRestart). Unknown to SDKs that predate it; their
	// decoder drops the field.
	Restarts []uint64 `json:"restarts,omitempty"`
}

// Counter of restart pages committed, summed across worker buckets; the
// per-sync restart limit is checked against it.
const LedgerActionRestarts = "ledger.action_restarts"

// RestartLimits bound how often a sync may restart work: PerAction is the
// most restarts one work item may accumulate; PerSync is the most restart
// pages the whole sync may commit. Zero is not unlimited; SetRestart
// rejects it.
type RestartLimits struct {
	PerAction uint64
	PerSync   uint64
}

// RestartScope names which of the two limits a RestartLimitError hit.
type RestartScope string

const (
	RestartScopeAction RestartScope = "action"
	RestartScopeSync   RestartScope = "sync"
)

var ErrRestartLimitExceeded = errors.New("restart limit exceeded")

// RestartLimitError is the refused restart: the batch did not commit and
// the pending item is unchanged. Count is the committed restarts in Scope,
// not the refused one; Restarts is the item's list at the time of the
// refusal.
type RestartLimitError struct {
	Scope    RestartScope
	Limit    uint64
	Count    uint64
	WorkID   uint64
	Restarts []uint64
}

func (e *RestartLimitError) Error() string {
	return fmt.Sprintf("%v: %s limit %d reached (%d restarts) on work %d; restarted at revisions %v",
		ErrRestartLimitExceeded, e.Scope, e.Limit, e.Count, e.WorkID, e.Restarts)
}

func (e *RestartLimitError) Unwrap() error { return ErrRestartLimitExceeded }
