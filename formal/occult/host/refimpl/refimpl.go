// Package refimpl is an executable REFERENCE implementation of the
// demand-graph runtime's per-scope sync loop (formal/GRAPH_MODEL_SPEC.md)
// — the "known good" algorithm, which has a frozen P model but no
// production implementation yet. It exists to be tried out: it runs the
// phantom-union scenario end to end, emits canonical traces in the
// TRACE_BRIDGE.md vocabulary, and carries a LEGACY mode reproducing the
// known-broken algorithm's two failure habits:
//
//  1. overlay grounding at the last sync's epoch instead of the replay
//     marker's attested base (the phantom-union composition), and
//  2. resume-without-regrounding: after a crash, a non-empty partition
//     is treated as already-replayed and overlaid directly, instead of
//     re-executing the node under a fresh generation.
//
// This is a modeling artifact, single-threaded and in-memory; it is
// deliberately NOT production code and lives outside baton-sdk's public
// module. The store commits writes durably as they happen (they survive
// a crash); the checkpoint is the scheduler watermark, exactly the
// walker/graph models' crash semantics.
package refimpl

import (
	"fmt"
	"sort"
)

// Mode selects the algorithm under test.
type Mode int

const (
	// ModeDemandGraph is the known-good algorithm: premise-validated
	// grounding, generation-bump re-execution on resume.
	ModeDemandGraph Mode = iota
	// ModeLegacy reproduces the known-broken algorithm.
	ModeLegacy
)

// Event is one canonical trace event (TRACE_BRIDGE.md vocabulary).
type Event struct {
	Kind  string // consult, clear, replay, upsert, publish, checkpoint, seal
	Scope string // "" for checkpoint/seal
}

// Upstream is the truthful system of record: a row set per epoch and
// honest diffs between any two epochs.
type Upstream struct {
	epochs []map[string]string
}

func NewUpstream(epochs ...map[string]string) *Upstream {
	return &Upstream{epochs: epochs}
}

func (u *Upstream) Rows(e int) map[string]string {
	out := map[string]string{}
	for k, v := range u.epochs[e] {
		out[k] = v
	}
	return out
}

// Diff returns the truthful delta from one epoch to another: upserts
// for added/changed ids, deletes for removed ids. It never mentions an
// id that did not change — which is exactly what makes the misgrounded
// composition dangerous.
func (u *Upstream) Diff(from, to int) (upserts map[string]string, deletes []string) {
	upserts = map[string]string{}
	a, b := u.epochs[from], u.epochs[to]
	for k, v := range b {
		if av, ok := a[k]; !ok || av != v {
			upserts[k] = v
		}
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			deletes = append(deletes, k)
		}
	}
	sort.Strings(deletes)
	return upserts, deletes
}

// Cache is the source-cache offer for the scope: rows attested at Base.
// The consult verdict truthfully reports Base.
type Cache struct {
	Base int
	Rows map[string]string
}

// Config describes one sync of one scope.
type Config struct {
	Mode          Mode
	Upstream      *Upstream
	Head          int   // the epoch this sync must land on
	Cache         Cache // the connector's replay offer
	LastSyncEpoch int   // what the previous completed sync attested (the legacy grounding source)
	// CrashAfterReplay kills attempt 1 after the replay unit commits
	// and before any checkpoint, then resumes as attempt 2.
	CrashAfterReplay bool
}

// Result is the sealed artifact content plus the canonical trace of
// every attempt (crash-cut traces included).
type Result struct {
	Sealed   map[string]string
	Attempts [][]Event
}

const scope = "s1"

// durable is the store state that survives a crash.
type durable struct {
	partition  map[string]string
	hasMarker  bool
	markerBase int  // the replay unit's attested base epoch
	checkpoint bool // scope watermark: scope completed at Head
}

// Run executes the sync to seal, crashing and resuming if configured.
func Run(cfg Config) Result {
	st := &durable{partition: map[string]string{}}
	var attempts [][]Event
	for attempt := 1; ; attempt++ {
		trace, sealed := runAttempt(cfg, st, attempt)
		attempts = append(attempts, trace)
		if sealed {
			return Result{Sealed: st.partition, Attempts: attempts}
		}
		if attempt > 2 {
			panic("refimpl: more than one resume in a two-attempt scenario")
		}
	}
}

func runAttempt(cfg Config, st *durable, attempt int) (trace []Event, sealed bool) {
	emit := func(kind, sc string) { trace = append(trace, Event{Kind: kind, Scope: sc}) }

	replayed := false
	switch {
	case cfg.Mode == ModeLegacy && len(st.partition) > 0 && attempt > 1:
		// LEGACY resume habit: a non-empty partition is treated as
		// already-replayed. No consult, no clear, no fresh generation —
		// straight to the overlay on top of whatever the dead attempt
		// left behind.
	default:
		// Demand-graph path (and legacy attempt 1): consult the source
		// cache; the verdict truthfully attests the cache's base epoch.
		emit("consult", scope)
		// Replay unit — atomic clear + copy + marker (the eGReplayUnit
		// shape: the marker commits with the copy or not at all).
		st.partition = map[string]string{}
		for k, v := range cfg.Cache.Rows {
			st.partition[k] = v
		}
		st.hasMarker = true
		st.markerBase = cfg.Cache.Base
		emit("clear", scope)
		emit("replay", scope)
		replayed = true
	}

	if cfg.CrashAfterReplay && attempt == 1 && replayed {
		// Crash: store writes above are durable; nothing was
		// checkpointed. The trace is cut here.
		return trace, false
	}

	// Overlay: bring the partition from the grounded base to Head.
	ground := st.markerBase // premise-validated: the marker's attestation
	if cfg.Mode == ModeLegacy {
		ground = cfg.LastSyncEpoch // the broken habit: last sync's epoch
	}
	upserts, deletes := cfg.Upstream.Diff(ground, cfg.Head)
	for _, k := range sortedKeys(upserts) {
		st.partition[k] = upserts[k]
		emit("upsert", scope)
	}
	for _, k := range deletes {
		delete(st.partition, k)
		// Tombstones have no canonical event yet (TRACE_BRIDGE.md
		// pending extensions); the content oracle still sees them.
	}

	emit("publish", scope)
	st.checkpoint = true
	emit("checkpoint", "")
	emit("seal", "")
	return trace, true
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// RenderOccult renders one attempt's trace as an Occult term over the
// sync_trace_policies constructors, with every constructor accessed
// through the module handle m (e.g. "M").
func RenderOccult(m string, trace []Event) string {
	term := m + ".tnil"
	for i := len(trace) - 1; i >= 0; i-- {
		ev := trace[i]
		var atom string
		switch ev.Kind {
		case "checkpoint":
			atom = fmt.Sprintf("%s.ev_checkpoint", m)
		case "seal":
			atom = fmt.Sprintf("%s.ev_seal", m)
		default:
			atom = fmt.Sprintf("%s.ev_%s(%s.%s)", m, ev.Kind, m, ev.Scope)
		}
		term = fmt.Sprintf("%s.tcons(%s, %s)", m, atom, term)
	}
	return term
}
