package chaosconnector

import (
	"fmt"
	"sync"
)

// Run binds immutable scenario truth to mutable execution state.
type Run struct {
	scenario  *Scenario
	runtime   *Runtime
	mutations *MutationRegistry

	mu    sync.RWMutex
	epoch string
}

// NewRun validates and arms one scenario execution.
func NewRun(scenario *Scenario, schedule Schedule) (*Run, error) {
	if err := scenario.Validate(); err != nil {
		return nil, err
	}
	runtime, err := NewRuntime(schedule, &Trace{})
	if err != nil {
		return nil, err
	}
	return &Run{
		scenario:  scenario,
		runtime:   runtime,
		mutations: NewMutationRegistry(),
		epoch:     scenario.InitialEpoch,
	}, nil
}

// Scenario returns the immutable scenario definition.
func (r *Run) Scenario() *Scenario {
	return r.scenario
}

// Runtime returns the run's fault runtime.
func (r *Run) Runtime() *Runtime {
	return r.runtime
}

// Mutations returns the run's response mutation registry.
func (r *Run) Mutations() *MutationRegistry {
	return r.mutations
}

// Trace returns the run's execution trace.
func (r *Run) Trace() *Trace {
	return r.runtime.trace
}

// Epoch returns the active temporal epoch.
func (r *Run) Epoch() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.epoch
}

// Dataset returns the active dataset.
func (r *Run) Dataset() *Dataset {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.scenario.Epochs[r.epoch]
}

// SetEpoch moves the connector to a declared temporal epoch.
func (r *Run) SetEpoch(epoch string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.scenario.Epochs[epoch]; !ok {
		return fmt.Errorf("chaosconnector: epoch %q does not exist", epoch)
	}
	r.epoch = epoch
	return nil
}
