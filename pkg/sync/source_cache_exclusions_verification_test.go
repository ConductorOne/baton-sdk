package sync //nolint:revive,nolintlint // backwards-compatible package name

import "testing"

// TestVerificationPhase6bExecutableExclusions is the Phase 6b registry of
// executable boundary exclusions for source-cache replay orchestration
// (docs/verification/sync-replay-6b/plan.md, B10). These are deliberate,
// registered scope boundaries — not silently omitted coverage. Removing an
// entry requires replacing it with evidence.
//
// Registered boundaries that HAVE instruments (listed here for the
// registry's completeness, not skipped):
//
//   - Static entitlements stay unscoped: SourceCacheRecord on a
//     ListStaticEntitlements response is ignored with a warn. Pinned by
//     TestChaosSourceCacheFreshPageSemantics/static-entitlement-annotation-ignored.
//   - Derived rows stay unscoped: expansion output is regenerated, never
//     stamped, and expander-written Sources are stripped at replay copy.
//     Pinned by TestChaosSourceCacheReplayStripsExpanderSources (stamp
//     counts exclude the expander-created grant).
func TestVerificationPhase6bExecutableExclusions(t *testing.T) {
	t.Run("lambda-ask-answer-continuation", func(t *testing.T) {
		t.Skip("SourceCacheLookupOffer is never attached to requests and SourceCacheLookupAsk/SourceCacheLookupAnswers are never consumed or produced " +
			"by 6b orchestration; the lambda ask/answer continuation is deferred to Phase 6c")
	})
	t.Run("subprocess-transport-lookup-delivery", func(t *testing.T) {
		t.Skip("CO-6b-001: subprocess-wrapped connectors (internal/connector's runner wrapper) have no RPC backchannel to receive the Lookup interface, " +
			"so they observe NoopLookup and sync cold; in-process delivery is covered by the gate-matrix suite's transport cells (direct client, in-process gRPC), " +
			"which are chaos-harness clients — no production transport wires the setter in this phase, and the syncer's deliverability probe keeps such syncs cold rather than logging warm")
	})
	t.Run("resource-targeted-sync-and-event-feeds", func(t *testing.T) {
		t.Skip("ResourceTargetedSyncer.Get and event feeds carry no source-cache semantics in Phase 6b (plan B10)")
	})
}
