// Real-sync-execution leg of the trace bridge (../TRACE_BRIDGE.md,
// mapping 2): JSONL trace fixtures recorded from REAL syncer executions
// by pkg/sync's chaos harness (chaos_trace_oracle_test.go, the
// testSyncTraceAudit recorder) are rendered onto the canonical event
// vocabulary and checked against all five deliverable-7 policies. This
// closes the loop the brief asked for: the same oracle that gates the P
// models' traces and the refimpl's traces now gates the shipped
// syncer's commit order.
//
// Rendering conventions (the recorder is purely observational; the
// conventions live here):
//   - Scopes: distinct (row_kind, scope_key) pairs map onto s1/s2 in
//     first-seen order — the policies' two-scope envelope. Fixtures
//     with more than two scopes are rejected.
//   - Structural clear: a NON-RESUMED attempt writes into partitions
//     StartNewSync created empty, so an upsert with no earlier explicit
//     clear for its scope gets an ev_clear inserted before it. Resumed
//     attempts inherit their predecessor's rows and get no such
//     insertion — exactly the case clear-before-upsert exists to catch.
//     Both committed fixtures are non-resumed; the convention is pinned
//     by the header's "resumed" field.
package host_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type realTraceHeader struct {
	Name    string `json:"name"`
	Resumed bool   `json:"resumed"`
}

type realTraceEvent struct {
	Kind     string `json:"kind"`
	RowKind  string `json:"row_kind"`
	ScopeKey string `json:"scope_key"`
}

// loadRealTrace parses one JSONL fixture: a header line then one event
// per line.
func loadRealTrace(t *testing.T, path string) (realTraceHeader, []realTraceEvent) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		t.Fatalf("%s: empty fixture", path)
	}
	var header realTraceHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		t.Fatalf("%s: header: %v", path, err)
	}
	var events []realTraceEvent
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var ev realTraceEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("%s: event: %v", path, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("%s: scan: %v", path, err)
	}
	if len(events) == 0 {
		t.Fatalf("%s: no events", path)
	}
	return header, events
}

// renderRealTrace converts a recorded event list into a canonical trace
// term over the policy module's constructors.
func renderRealTrace(t *testing.T, header realTraceHeader, events []realTraceEvent) string {
	t.Helper()
	scopes := map[string]string{}
	scopeName := func(ev realTraceEvent) string {
		key := ev.RowKind + "\x00" + ev.ScopeKey
		if name, ok := scopes[key]; ok {
			return name
		}
		name := fmt.Sprintf("s%d", len(scopes)+1)
		if len(scopes) >= 2 {
			t.Fatalf("fixture %s has more than two scopes (policies' two-scope envelope)", header.Name)
		}
		scopes[key] = name
		return name
	}
	cleared := map[string]bool{}
	var canonical []string
	for _, ev := range events {
		switch ev.Kind {
		case "checkpoint":
			canonical = append(canonical, "ev_checkpoint")
		case "seal":
			canonical = append(canonical, "ev_seal")
		case "consult", "clear", "replay", "upsert", "publish":
			s := scopeName(ev)
			if ev.Kind == "clear" {
				cleared[s] = true
			}
			if ev.Kind == "upsert" && !cleared[s] && !header.Resumed {
				// Structural clear: the partition was born empty this
				// attempt (see the file comment).
				canonical = append(canonical, "ev_clear(M."+s+")")
				cleared[s] = true
			}
			canonical = append(canonical, fmt.Sprintf("ev_%s(M.%s)", ev.Kind, s))
		default:
			t.Fatalf("fixture %s: unknown event kind %q", header.Name, ev.Kind)
		}
	}
	term := "M.tnil"
	for i := len(canonical) - 1; i >= 0; i-- {
		term = fmt.Sprintf("M.tcons(M.%s, %s)", canonical[i], term)
	}
	return term
}

// TestRealSyncTracesSatisfyPolicies checks every committed real-trace
// fixture against all five policies.
func TestRealSyncTracesSatisfyPolicies(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "realtraces", "*.jsonl"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no real-trace fixtures committed under testdata/realtraces")
	}
	for _, path := range paths {
		header, events := loadRealTrace(t, path)
		term := renderRealTrace(t, header, events)
		for _, policy := range policies {
			t.Run(header.Name+"/"+policy, func(t *testing.T) {
				verdict := policyVerdictTerm(t, policy, term)
				if verdict != "ok" {
					t.Errorf("real trace %s violates %s: %q\nterm: %s", header.Name, policy, verdict, term)
				}
			})
		}
	}
}

// TestRealTraceBridgeCatchesPlantedViolation validates the bridge
// itself (instrument validation: the oracle must fail on a planted
// violation, or a rendering bug that greens everything would pass
// silently). It mutates the warm fixture's REAL events two ways:
// dropping the consult must red consult-before-replay, and replaying
// the same events as a RESUMED attempt with an upsert in place of the
// replay unit must red clear-before-upsert (no structural clear).
func TestRealTraceBridgeCatchesPlantedViolation(t *testing.T) {
	path := filepath.Join("testdata", "realtraces", "warm_replay_sync.jsonl")
	header, events := loadRealTrace(t, path)

	var noConsult []realTraceEvent
	for _, ev := range events {
		if ev.Kind == "consult" {
			continue
		}
		noConsult = append(noConsult, ev)
	}
	term := renderRealTrace(t, header, noConsult)
	if verdict := policyVerdictTerm(t, "consult_before_replay", term); verdict != "violation: consult-before-replay" {
		t.Errorf("planted consult drop not caught, verdict %q", verdict)
	}

	resumed := realTraceHeader{Name: "planted-resume", Resumed: true}
	var upsertOnly []realTraceEvent
	for _, ev := range events {
		switch ev.Kind {
		case "clear", "replay", "consult":
			if ev.Kind == "replay" {
				upsertOnly = append(upsertOnly, realTraceEvent{Kind: "upsert", RowKind: ev.RowKind, ScopeKey: ev.ScopeKey})
			}
		default:
			upsertOnly = append(upsertOnly, ev)
		}
	}
	term = renderRealTrace(t, resumed, upsertOnly)
	if verdict := policyVerdictTerm(t, "clear_before_upsert", term); verdict != "violation: clear-before-upsert" {
		t.Errorf("planted un-regrounded resume not caught, verdict %q", verdict)
	}
}
