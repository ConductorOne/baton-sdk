package chaosconnector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const ScheduleVersion = 1

// EffectKind identifies a replayable disturbance.
type EffectKind string

const (
	EffectError        EffectKind = "error"
	EffectDelay        EffectKind = "delay"
	EffectBlock        EffectKind = "block"
	EffectCancel       EffectKind = "cancel"
	EffectLoseResponse EffectKind = "lose-response"
	EffectMutate       EffectKind = "mutate"
	EffectSetEpoch     EffectKind = "set-epoch"
	// EffectCrash is a cooperative in-process interruption. It returns
	// ErrInterruptRequested; only process-capable harnesses may translate it
	// into an actual hard crash.
	EffectCrash EffectKind = "crash"
)

// Effect describes one replayable disturbance. Mutation names are resolved by
// the response-mutation registry; they are not arbitrary Go callbacks.
type Effect struct {
	Kind     EffectKind `json:"kind"`
	Code     codes.Code `json:"code,omitempty"`
	Message  string     `json:"message,omitempty"`
	Delay    int64      `json:"delay_ms,omitempty"`
	Mutation string     `json:"mutation,omitempty"`
	Epoch    string     `json:"epoch,omitempty"`
	Barrier  string     `json:"barrier,omitempty"`
}

// Rule applies its effects to matching operations. MinFires makes non-vacuity
// part of the schedule contract. MaxFires zero means unlimited.
type Rule struct {
	ID       string   `json:"id"`
	Match    Matcher  `json:"match"`
	Effects  []Effect `json:"effects"`
	MinFires int      `json:"min_fires,omitempty"`
	MaxFires int      `json:"max_fires,omitempty"`
}

// Schedule is the serializable fault program for one run.
type Schedule struct {
	Version int    `json:"version"`
	Rules   []Rule `json:"rules,omitempty"`
}

// NewSchedule constructs the current schedule format.
func NewSchedule(rules ...Rule) Schedule {
	return Schedule{Version: ScheduleVersion, Rules: append([]Rule(nil), rules...)}
}

// Validate rejects schedules that cannot provide useful replay evidence.
func (s Schedule) Validate() error {
	if s.Version != ScheduleVersion {
		return fmt.Errorf("chaosconnector: unsupported schedule version %d", s.Version)
	}
	seen := make(map[string]struct{}, len(s.Rules))
	for i, rule := range s.Rules {
		if rule.ID == "" {
			return fmt.Errorf("chaosconnector: rule %d has no id", i)
		}
		if _, ok := seen[rule.ID]; ok {
			return fmt.Errorf("chaosconnector: duplicate rule id %q", rule.ID)
		}
		seen[rule.ID] = struct{}{}
		if len(rule.Effects) == 0 {
			return fmt.Errorf("chaosconnector: rule %q has no effects", rule.ID)
		}
		if rule.MinFires < 0 || rule.MaxFires < 0 {
			return fmt.Errorf("chaosconnector: rule %q has a negative fire bound", rule.ID)
		}
		if rule.MaxFires > 0 && rule.MinFires > rule.MaxFires {
			return fmt.Errorf("chaosconnector: rule %q requires more fires than it allows", rule.ID)
		}
		for _, effect := range rule.Effects {
			if err := effect.validate(); err != nil {
				return fmt.Errorf("chaosconnector: rule %q: %w", rule.ID, err)
			}
		}
	}
	return nil
}

func (e Effect) validate() error {
	switch e.Kind {
	case EffectError, EffectDelay, EffectBlock, EffectCancel, EffectLoseResponse,
		EffectMutate, EffectSetEpoch, EffectCrash:
	default:
		return fmt.Errorf("unknown effect %q", e.Kind)
	}
	if e.Delay < 0 {
		return errors.New("delay cannot be negative")
	}
	if e.Kind == EffectMutate && e.Mutation == "" {
		return errors.New("mutation effect has no registry name")
	}
	if e.Kind == EffectSetEpoch && e.Epoch == "" {
		return errors.New("epoch effect has no epoch")
	}
	return nil
}

// FiredRule is a matched rule and an isolated copy of its effects.
type FiredRule struct {
	ID      string
	Effects []Effect
}

// Runtime owns per-run attempts, fire counts, barriers, and trace state.
type Runtime struct {
	schedule Schedule
	trace    *Trace

	mu       sync.Mutex
	attempts map[string]int
	fires    map[string]int
	barriers map[string]chan struct{}
	active   int
}

func (r *Runtime) operationStarted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active++
}

func (r *Runtime) operationFinished() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active--
}

// ActiveOperations reports connector calls still executing inside the fault
// wrapper. Cancellation tests use it to distinguish bounded return from a
// leaked blocked call.
func (r *Runtime) ActiveOperations() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active
}

// NewRuntime validates and arms a schedule.
func NewRuntime(schedule Schedule, trace *Trace) (*Runtime, error) {
	if err := schedule.Validate(); err != nil {
		return nil, err
	}
	return &Runtime{
		schedule: schedule,
		trace:    trace,
		attempts: make(map[string]int),
		fires:    make(map[string]int),
		barriers: make(map[string]chan struct{}),
	}, nil
}

// Begin assigns the next one-based attempt to an operation.
func (r *Runtime) Begin(op Operation) Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := op.LogicalKey()
	r.attempts[key]++
	op.Attempt = r.attempts[key]
	return op
}

// Match returns every rule armed for op in schedule order.
func (r *Runtime) Match(op Operation) []FiredRule {
	r.mu.Lock()
	defer r.mu.Unlock()

	var matched []FiredRule
	for _, rule := range r.schedule.Rules {
		if !rule.Match.Matches(op) {
			continue
		}
		if rule.MaxFires > 0 && r.fires[rule.ID] >= rule.MaxFires {
			continue
		}
		r.fires[rule.ID]++
		matched = append(matched, FiredRule{
			ID:      rule.ID,
			Effects: append([]Effect(nil), rule.Effects...),
		})
	}
	return matched
}

// ApplyControlEffects executes effects that do not require a protobuf response.
// It returns the first injected terminal error.
func (r *Runtime) ApplyControlEffects(ctx context.Context, op Operation, fired []FiredRule) error {
	for _, match := range fired {
		for _, effect := range match.Effects {
			r.trace.Record(TraceEvent{
				Operation: op,
				RuleID:    match.ID,
				Effect:    effect.Kind,
				Outcome:   OutcomeInjected,
			})
			switch effect.Kind {
			case EffectError:
				code := effect.Code
				if code == codes.OK {
					code = codes.Unknown
				}
				return status.Error(code, effect.Message)
			case EffectLoseResponse:
				code := effect.Code
				if code == codes.OK {
					code = codes.Unavailable
				}
				message := effect.Message
				if message == "" {
					message = "response lost after delegate"
				}
				return status.Error(code, message)
			case EffectDelay:
				timer := time.NewTimer(time.Duration(effect.Delay) * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			case EffectBlock:
				select {
				case <-r.barrier(effect.Barrier):
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			case EffectCancel:
				return context.Canceled
			case EffectCrash:
				return ErrInterruptRequested
			case EffectMutate, EffectSetEpoch:
				// Response and scenario adapters own these effects.
			}
		}
	}
	return nil
}

func (r *Runtime) barrier(name string) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.barriers[name]
	if !ok {
		ch = make(chan struct{})
		r.barriers[name] = ch
	}
	return ch
}

// ReleaseBarrier releases all current and future waiters for name.
func (r *Runtime) ReleaseBarrier(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.barriers[name]
	if !ok {
		ch = make(chan struct{})
		r.barriers[name] = ch
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// VerifyRequired reports every schedule rule that did not meet its minimum.
func (r *Runtime) VerifyRequired() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var missing []error
	for _, rule := range r.schedule.Rules {
		if r.fires[rule.ID] < rule.MinFires {
			missing = append(missing, fmt.Errorf(
				"rule %q fired %d times, requires %d",
				rule.ID,
				r.fires[rule.ID],
				rule.MinFires,
			))
		}
	}
	return errors.Join(missing...)
}

// FireCounts returns an isolated snapshot keyed by rule ID.
func (r *Runtime) FireCounts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.fires))
	for id, count := range r.fires {
		out[id] = count
	}
	return out
}

// ErrInterruptRequested asks the current run to stop at a deterministic seam.
// It is not evidence of process-death durability unless a process harness
// translates it into an OS-level termination.
var ErrInterruptRequested = errors.New("chaosconnector: interruption requested")

// ErrCrashRequested is retained for source compatibility.
//
// Deprecated: use ErrInterruptRequested.
var ErrCrashRequested = ErrInterruptRequested
