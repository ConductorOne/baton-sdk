// Real-sync-execution leg of the trace bridge (../TRACE_BRIDGE.md,
// mapping 2): JSONL trace fixtures recorded from REAL syncer executions
// by pkg/sync's chaos harness (chaos_trace_oracle_test.go, the
// testSyncTraceAudit recorder) are rendered onto the canonical event
// vocabulary and checked against all six deliverable-7 policies. This
// closes the loop the brief asked for: the same oracle that gates the P
// models' traces and the refimpl's traces now gates the shipped
// syncer's commit order.
//
// KNOWN-DEFECT PIN: the session-zombie fixture (recorded by
// pkg/sync's TestChaosSourceCacheSessionPersistsAcrossResume) is
// EXPECTED RED on session_ckpt_consistency — sessions commit durably
// at op time, outside the checkpoint mechanism, so a crashed attempt's
// beyond-checkpoint write survives the cursor rollback and the re-run
// reads it (CO-6b-009). The red verdict on a real execution IS the
// mechanical catch of the shipped defect. When checkpoint-consistent
// sessions land (the registered future work), the recorded trace
// becomes a read-miss and this expectation flips to "ok".
//
// Rendering conventions (the recorder is purely observational; the
// conventions live here):
//   - Scopes: distinct (row_kind, scope_key) pairs map onto s1/s2 in
//     first-seen order — the policies' two-scope envelope. Fixtures
//     with more than two scopes are rejected.
//   - Structural clear: a trace that starts at sync birth
//     (header resumed=false) writes into partitions StartNewSync
//     created empty, so an upsert with no earlier explicit clear for
//     its scope gets an ev_clear inserted before it — once per scope
//     for the WHOLE sync trace, attempts included (the partition is
//     born empty once). A trace beginning mid-sync (resumed=true)
//     gets no insertion — exactly the case clear-before-upsert exists
//     to catch. All committed fixtures start at sync birth.
//   - Resume markers: multi-attempt fixtures carry {"kind":"resume"}
//     lines between attempt segments, rendered as ev_resume.
//   - Checkpoint coalescing: a run of consecutive checkpoints renders
//     as ONE ev_checkpoint. Verdict-preserving by inspection of all
//     five policies: four pass checkpoints through untouched and the
//     fifth (checkpoint-before-progress) sets an idempotent flag, so
//     back-to-back checkpoints are indistinguishable from one. Needed
//     because the engine's term evaluation cost grows steeply with
//     event count (a 25-event trace exceeds 18 minutes per cell; 14
//     events evaluate in tens of seconds).
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
	sessionKey := ""
	var canonical []string
	for _, ev := range events {
		switch ev.Kind {
		case "checkpoint":
			if len(canonical) > 0 && canonical[len(canonical)-1] == "ev_checkpoint" {
				continue
			}
			canonical = append(canonical, "ev_checkpoint")
		case "seal":
			canonical = append(canonical, "ev_seal")
		case "resume":
			canonical = append(canonical, "ev_resume")
		case "swrite", "sread_hit", "sread_miss":
			// Session events map onto the policy module's one-key
			// envelope (k1), separate from the artifact scopes.
			if sessionKey == "" {
				sessionKey = ev.ScopeKey
			}
			if ev.ScopeKey != sessionKey {
				t.Fatalf("fixture %s has more than one session key (policies' one-key envelope)", header.Name)
			}
			canonical = append(canonical, "ev_"+ev.Kind+"(M.k1)")
		case "consult", "clear", "replay", "upsert", "delete", "publish":
			s := scopeName(ev)
			if ev.Kind == "clear" {
				cleared[s] = true
			}
			if (ev.Kind == "upsert" || ev.Kind == "delete") && !cleared[s] && !header.Resumed {
				// Structural clear: the partition was born empty this
				// sync (see the file comment). Deletes are writes and
				// need the same grounding as upserts.
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

// realTraceExpected overrides the default "ok" expectation for
// (fixture, policy) cells. The only entry is the known-defect pin (see
// the file comment): the shipped session semantics' zombie read, RED
// on a real execution's trace by design until checkpoint-consistent
// sessions land.
var realTraceExpected = map[string]map[string]string{
	"warm_replay_sync_session_zombie": {
		"session_ckpt_consistency": "violation: session-zombie-read",
	},
}

// TestRealSyncTracesSatisfyPolicies checks every committed real-trace
// fixture against all six policies.
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
				want := "ok"
				if overrides, ok := realTraceExpected[header.Name]; ok {
					if v, ok := overrides[policy]; ok {
						want = v
					}
				}
				verdict := policyVerdictTerm(t, policy, term)
				if verdict != want {
					t.Errorf("real trace %s under %s: want %q, got %q\nterm: %s", header.Name, policy, want, verdict, term)
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

// TestRealTraceBridgeCatchesUngroundedDelete validates the delete leg
// of the bridge: the tombstone fixture's REAL delete, replayed as a
// mid-sync trace with its grounding (clear+replay) and upsert stripped,
// must red clear-before-upsert — a tombstone against a base this trace
// never copied is the un-regrounded-resume class, delete flavor.
func TestRealTraceBridgeCatchesUngroundedDelete(t *testing.T) {
	path := filepath.Join("testdata", "realtraces", "warm_replay_sync_tombstone.jsonl")
	header, events := loadRealTrace(t, path)

	term := renderRealTrace(t, header, events)
	if verdict := policyVerdictTerm(t, "clear_before_upsert", term); verdict != "ok" {
		t.Errorf("honest tombstone trace must satisfy clear-before-upsert, got %q", verdict)
	}

	hasDelete := false
	var ungrounded []realTraceEvent
	for _, ev := range events {
		switch ev.Kind {
		case "clear", "replay", "upsert":
			continue
		case "delete":
			hasDelete = true
		}
		ungrounded = append(ungrounded, ev)
	}
	if !hasDelete {
		t.Fatal("tombstone fixture carries no delete event; the delete leg is not being exercised")
	}
	term = renderRealTrace(t, realTraceHeader{Name: "planted-ungrounded-delete", Resumed: true}, ungrounded)
	if verdict := policyVerdictTerm(t, "clear_before_upsert", term); verdict != "violation: clear-before-upsert" {
		t.Errorf("planted ungrounded delete not caught, verdict %q", verdict)
	}
}

// TestRealTraceBridgeResumeMarkerLoadBearing validates the multi-attempt
// leg of the bridge: the interrupted fixture's two replays are legal
// ONLY because a resume marker separates them (once-per-scope resets at
// the boundary). Deleting the marker must turn the same events into a
// within-attempt duplicate copy and red once-per-scope — proving the
// marker, and therefore the attempt segmentation, is load-bearing.
func TestRealTraceBridgeResumeMarkerLoadBearing(t *testing.T) {
	path := filepath.Join("testdata", "realtraces", "warm_replay_sync_interrupted.jsonl")
	header, events := loadRealTrace(t, path)

	hasResume := false
	var noMarker []realTraceEvent
	for _, ev := range events {
		if ev.Kind == "resume" {
			hasResume = true
			continue
		}
		noMarker = append(noMarker, ev)
	}
	if !hasResume {
		t.Fatal("interrupted fixture carries no resume marker; the multi-attempt leg is not being exercised")
	}

	term := renderRealTrace(t, header, events)
	if verdict := policyVerdictTerm(t, "once_per_scope", term); verdict != "ok" {
		t.Errorf("marked multi-attempt trace must satisfy once-per-scope, got %q", verdict)
	}
	term = renderRealTrace(t, header, noMarker)
	if verdict := policyVerdictTerm(t, "once_per_scope", term); verdict != "violation: once-per-scope" {
		t.Errorf("marker-stripped trace must red once-per-scope, got %q", verdict)
	}
}
