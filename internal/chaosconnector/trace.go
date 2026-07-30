package chaosconnector

import "sync"

// Outcome records the externally visible result of one trace event.
type Outcome string

const (
	OutcomeObserved Outcome = "observed"
	OutcomeInjected Outcome = "injected"
	OutcomeReturned Outcome = "returned"
	OutcomeErrored  Outcome = "errored"
)

// TraceEvent is one immutable observation made by the fault runtime.
type TraceEvent struct {
	Operation Operation  `json:"operation"`
	RuleID    string     `json:"rule_id,omitempty"`
	Effect    EffectKind `json:"effect,omitempty"`
	Outcome   Outcome    `json:"outcome"`
	Error     string     `json:"error,omitempty"`
}

// Trace is a concurrency-safe execution log.
type Trace struct {
	mu     sync.Mutex
	events []TraceEvent
}

// Record appends an event.
func (t *Trace) Record(event TraceEvent) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}

// Events returns an isolated snapshot in observed order.
func (t *Trace) Events() []TraceEvent {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]TraceEvent(nil), t.events...)
}
