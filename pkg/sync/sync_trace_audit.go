package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	native_sync "sync"
)

// syncTraceAudit is the test-only canonical-event recorder behind the
// formal trace-policy oracle (formal/occult/TRACE_BRIDGE.md, "Mapping 2:
// real sync executions"). The ordering/durability policies verified over
// the P models' traces (formal/occult/src/sync_trace_policies.occult)
// consume event lists in a canonical vocabulary; recording at the
// syncer's source-cache orchestration seams lets chaos tests export real
// sync traces to the same oracle, closing the model-to-implementation
// bridge the formal brief requires.
//
// The recorder is purely observational: every event corresponds to a
// store operation that actually committed, in commit order. Rendering
// conventions (e.g. the structural emptiness of a brand-new sync's
// partitions standing in for an explicit clear) belong to the trace
// renderer on the oracle side, never here.
//
// Production cost is one nil pointer check per recorded event: the
// syncer only carries an audit when a test sets testSyncTraceAudit
// (same pattern as testQueueAudit).
type syncTraceAudit struct {
	mu     native_sync.Mutex
	events []syncTraceEvent
}

type syncTraceKind string

const (
	// syncTraceConsult: the previous-artifact source-cache lookup
	// resolved (hit or miss) for a scope.
	syncTraceConsult syncTraceKind = "consult"
	// syncTraceClear: the replay unit's destination-scope clear
	// committed (recorded with syncTraceReplay in the store's
	// contractual clear-then-copy leg order).
	syncTraceClear syncTraceKind = "clear"
	// syncTraceReplay: a ReplaySourceCache replacement copy committed.
	syncTraceReplay syncTraceKind = "replay"
	// syncTraceUpsert: a scoped page's row upserts committed (page
	// granularity — the policies gate ordering, not row counts).
	syncTraceUpsert syncTraceKind = "upsert"
	// syncTraceDelete: a scoped page's tombstones committed (one event
	// per successful store delete call: canonical-id and
	// principal-scoped fire separately, in their contractual order).
	syncTraceDelete syncTraceKind = "delete"
	// syncTracePublish: the scope's manifest entry (validator) was
	// written.
	syncTracePublish syncTraceKind = "publish"
	// syncTraceCheckpoint: a checkpoint token durably committed.
	syncTraceCheckpoint syncTraceKind = "checkpoint"
	// syncTraceSeal: EndSync succeeded.
	syncTraceSeal syncTraceKind = "seal"
	// syncTraceSessionWrite: a connector session write committed.
	// Sessions are durable at op time, OUTSIDE the checkpoint
	// mechanism — the provenance hazard the oracle's policy 6
	// (session-checkpoint consistency) judges. The chaos connector has
	// no session plumbing, so these events are recorded by the test
	// acting as the session actor, at the moment of its real store
	// operation (see TestChaosSourceCacheSessionPersistsAcrossResume).
	syncTraceSessionWrite syncTraceKind = "swrite"
	// syncTraceSessionReadHit / syncTraceSessionReadMiss: a session
	// read returning found / not-found. ScopeKey carries the session
	// key for session events.
	syncTraceSessionReadHit  syncTraceKind = "sread_hit"
	syncTraceSessionReadMiss syncTraceKind = "sread_miss"
)

// syncTraceEvent is one canonical event. Scope-less kinds (checkpoint,
// seal) leave RowKind and ScopeKey empty.
type syncTraceEvent struct {
	Kind     syncTraceKind `json:"kind"`
	RowKind  string        `json:"row_kind,omitempty"`
	ScopeKey string        `json:"scope_key,omitempty"`
}

// record appends one event. Nil-receiver safe so call sites need no
// guard; the mutex serializes concurrent workers, and because every
// call site sits directly after its operation's commit, the recorded
// order is a real commit order (one legal linearization of the sync).
func (a *syncTraceAudit) record(kind syncTraceKind, rowKind, scopeKey string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, syncTraceEvent{Kind: kind, RowKind: rowKind, ScopeKey: scopeKey})
}

// snapshot returns a copy of the recorded events.
func (a *syncTraceAudit) snapshot() []syncTraceEvent {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]syncTraceEvent, len(a.events))
	copy(out, a.events)
	return out
}
