//go:build batonsdkv2

// Package equivalence runs the same workload through two storage
// backends and asserts they produce equivalent results across every
// reader method (RFC 0004 Stack 5).
//
// Stack 5 MVP scope: a small property-test harness that drives the
// canonical GrantRecord path through both the v3 Pebble engine and a
// reference implementation (a deterministic in-memory map). For each
// case we Put N grants, then for each reader method (GetGrantRecord,
// IterateGrantsBySync, IterateGrantsByEntitlement, IterateGrantsByPrincipal)
// we compare the returned set against the reference.
//
// The runner is intentionally pluggable: future stacks (Stack 6+) can
// register the SQLite engine as a second `Reference` once the
// v2 ↔ v3 translation layer is implemented.
package equivalence

import (
	"context"
	"errors"
	"fmt"
	"sort"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// Reference is the minimal store contract the equivalence runner
// drives. Both the Pebble engine and the in-memory reference satisfy
// it; future SQLite + translation shims can be wired in by adding a
// new adapter.
type Reference interface {
	PutGrantRecord(ctx context.Context, r *v3.GrantRecord) error
	GetGrantRecord(ctx context.Context, syncID, externalID string) (*v3.GrantRecord, error)
	IterateGrantsBySync(ctx context.Context, syncID string, yield func(*v3.GrantRecord) bool) error
	IterateGrantsByEntitlement(ctx context.Context, syncID, entitlementID string, yield func(*v3.GrantRecord) bool) error
	IterateGrantsByPrincipal(ctx context.Context, syncID, principalRT, principalID string, yield func(*v3.GrantRecord) bool) error
}

// Workload is a deterministic sequence of operations the runner
// replays into each Reference. Each op is replayed in order; the
// runner does not assume associativity or commutativity.
type Workload struct {
	Name   string
	SyncID string
	Ops    []Op
}

// OpKind discriminates Op types.
type OpKind int

const (
	OpPut OpKind = iota
	OpDelete
)

// Op is one operation in a workload.
type Op struct {
	Kind             OpKind
	Record           *v3.GrantRecord // nil for OpDelete
	DeleteExternalID string
}

// Result is the per-Reference output collected at the end of a
// workload, in a form that's directly comparable across implementations.
type Result struct {
	BySyncCount        int
	ByEntitlementCount map[string]int
	ByPrincipalCount   map[string]int
	GetSpotChecks      map[string]bool // externalID -> present
}

// RunWorkload replays w against ref and collects a comparable Result.
// The runner is deterministic; running the same workload twice on the
// same reference must produce the same Result.
func RunWorkload(ctx context.Context, ref Reference, w Workload) (*Result, error) {
	if cs, ok := ref.(currentSyncSetter); ok {
		if err := cs.SetCurrentSync(w.SyncID); err != nil {
			return nil, fmt.Errorf("equivalence: SetCurrentSync: %w", err)
		}
	}

	// Track keys we expect to read back.
	expectedExternal := map[string]bool{}
	entitlements := map[string]bool{}
	principalsRT := map[string]bool{}
	principalsID := map[string]bool{}

	for i, op := range w.Ops {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch op.Kind {
		case OpPut:
			if op.Record == nil {
				return nil, fmt.Errorf("equivalence: op[%d] OpPut with nil record", i)
			}
			if err := ref.PutGrantRecord(ctx, op.Record); err != nil {
				return nil, fmt.Errorf("equivalence: op[%d] PutGrantRecord: %w", i, err)
			}
			expectedExternal[op.Record.GetExternalId()] = true
			entitlements[op.Record.GetEntitlement().GetEntitlementId()] = true
			principalsRT[op.Record.GetPrincipal().GetResourceTypeId()] = true
			principalsID[op.Record.GetPrincipal().GetResourceId()] = true
		case OpDelete:
			if d, ok := ref.(deleter); ok {
				if err := d.DeleteGrantRecord(ctx, w.SyncID, op.DeleteExternalID); err != nil {
					return nil, fmt.Errorf("equivalence: op[%d] DeleteGrantRecord: %w", i, err)
				}
			}
			delete(expectedExternal, op.DeleteExternalID)
		default:
			return nil, fmt.Errorf("equivalence: op[%d] unknown kind %v", i, op.Kind)
		}
	}

	res := &Result{
		ByEntitlementCount: map[string]int{},
		ByPrincipalCount:   map[string]int{},
		GetSpotChecks:      map[string]bool{},
	}

	// IterateGrantsBySync
	if err := ref.IterateGrantsBySync(ctx, w.SyncID, func(*v3.GrantRecord) bool {
		res.BySyncCount++
		return true
	}); err != nil {
		return nil, fmt.Errorf("equivalence: IterateGrantsBySync: %w", err)
	}

	// IterateGrantsByEntitlement, deterministic order
	entSlice := make([]string, 0, len(entitlements))
	for k := range entitlements {
		entSlice = append(entSlice, k)
	}
	sort.Strings(entSlice)
	for _, ent := range entSlice {
		count := 0
		if err := ref.IterateGrantsByEntitlement(ctx, w.SyncID, ent, func(*v3.GrantRecord) bool {
			count++
			return true
		}); err != nil {
			return nil, fmt.Errorf("equivalence: IterateGrantsByEntitlement(%q): %w", ent, err)
		}
		res.ByEntitlementCount[ent] = count
	}

	// IterateGrantsByPrincipal — flatten (rt, id) cross product to all
	// (rt,id) pairs that appeared in the workload. This way we exercise
	// both lookup paths and we don't have an explosion of empty calls.
	for rt := range principalsRT {
		for id := range principalsID {
			count := 0
			if err := ref.IterateGrantsByPrincipal(ctx, w.SyncID, rt, id, func(*v3.GrantRecord) bool {
				count++
				return true
			}); err != nil {
				return nil, fmt.Errorf("equivalence: IterateGrantsByPrincipal(%q,%q): %w", rt, id, err)
			}
			if count > 0 {
				res.ByPrincipalCount[rt+"/"+id] = count
			}
		}
	}

	// Get spot-checks: for each expected external_id, confirm it
	// returns a non-nil record.
	for ext := range expectedExternal {
		r, err := ref.GetGrantRecord(ctx, w.SyncID, ext)
		switch {
		case err == nil && r != nil:
			res.GetSpotChecks[ext] = true
		case err == nil && r == nil:
			res.GetSpotChecks[ext] = false
		default:
			// missing: record absent. Reference's job to surface that.
			res.GetSpotChecks[ext] = false
		}
	}

	return res, nil
}

// Equal returns nil iff a and b are byte-equal Results, or a
// descriptive error otherwise. Used by the test driver to assert
// engine equivalence.
func Equal(a, b *Result) error {
	if a.BySyncCount != b.BySyncCount {
		return fmt.Errorf("BySyncCount: a=%d b=%d", a.BySyncCount, b.BySyncCount)
	}
	if !mapEq(a.ByEntitlementCount, b.ByEntitlementCount) {
		return fmt.Errorf("ByEntitlementCount: a=%v b=%v", a.ByEntitlementCount, b.ByEntitlementCount)
	}
	if !mapEq(a.ByPrincipalCount, b.ByPrincipalCount) {
		return fmt.Errorf("ByPrincipalCount: a=%v b=%v", a.ByPrincipalCount, b.ByPrincipalCount)
	}
	if !boolMapEq(a.GetSpotChecks, b.GetSpotChecks) {
		return fmt.Errorf("GetSpotChecks: a=%v b=%v", a.GetSpotChecks, b.GetSpotChecks)
	}
	return nil
}

func mapEq(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func boolMapEq(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// Compare runs w against both refA and refB and returns nil if their
// outputs match. Used by the test driver to assert engine equivalence.
func Compare(ctx context.Context, refA, refB Reference, w Workload) error {
	resA, err := RunWorkload(ctx, refA, w)
	if err != nil {
		return fmt.Errorf("workload %q on A: %w", w.Name, err)
	}
	resB, err := RunWorkload(ctx, refB, w)
	if err != nil {
		return fmt.Errorf("workload %q on B: %w", w.Name, err)
	}
	if err := Equal(resA, resB); err != nil {
		return fmt.Errorf("workload %q: %w", w.Name, err)
	}
	return nil
}

// deleter is an optional capability — the reference can implement
// it to support OpDelete, otherwise OpDelete is a no-op against the
// reference.
type deleter interface {
	DeleteGrantRecord(ctx context.Context, syncID, externalID string) error
}

// currentSyncSetter is an optional capability — references that track
// a "current sync" (like the Pebble engine) implement it so empty-
// syncID reads route correctly. References that don't need it (like
// the in-memory map) simply omit it.
type currentSyncSetter interface {
	SetCurrentSync(syncID string) error
}

// ErrNotImplemented is returned by reference adapters that don't
// support a method.
var ErrNotImplemented = errors.New("equivalence: reference doesn't implement this method")
