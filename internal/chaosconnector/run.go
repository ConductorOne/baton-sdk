package chaosconnector

import (
	"fmt"
	"sync"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// Run binds immutable scenario truth to mutable execution state.
type Run struct {
	scenario  *Scenario
	runtime   *Runtime
	mutations *MutationRegistry

	mu    sync.RWMutex
	epoch string
	// sourceCacheCapability is the capability annotation the reference
	// connector attaches to Validate responses; nil means not declared.
	// Mutable so generational suites can rotate it between syncs.
	sourceCacheCapability *v2.SourceCacheCapability
	// sourceCacheEvents records connector-side lookup consults in serve
	// order (see source_cache.go).
	sourceCacheEvents []SourceCacheLookupEvent
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
	isolated := cloneScenario(scenario)
	return &Run{
		scenario:  isolated,
		runtime:   runtime,
		mutations: NewMutationRegistry(),
		epoch:     isolated.InitialEpoch,
	}, nil
}

// Scenario returns an isolated copy of the scenario definition.
func (r *Run) Scenario() *Scenario {
	return cloneScenario(r.scenario)
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

// Dataset returns an isolated copy of the active dataset.
func (r *Run) Dataset() *Dataset {
	return cloneDataset(r.dataset())
}

func (r *Run) dataset() *Dataset {
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
