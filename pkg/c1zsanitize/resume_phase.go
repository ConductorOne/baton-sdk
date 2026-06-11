package c1zsanitize

import "encoding/json"

// ResumePhase reads the sanitize resume phase encoded in a destination sync's
// raw sync_token. It returns the phase name (one of "resources",
// "entitlements", "grants", "assets"), the grant page token (meaningful only at
// the grants phase), the source sync id the token correlates to, and ok=false
// when the token is empty or not a c1zsanitize checkpoint. It does NOT validate
// the secret fingerprint and performs no I/O — it is a pure decoder over the
// token string a caller already holds (e.g. from dotc1z.SyncRun.SyncToken or
// CurrentSyncStep), so a consumer does not have to couple to the unexported
// token JSON shape.
//
//nolint:nonamedreturns // the named results document the decoded token fields.
func ResumePhase(syncToken string) (phase, grantPage, srcSyncID string, ok bool) {
	if syncToken == "" {
		return "", "", "", false
	}
	var ct checkpointToken
	if json.Unmarshal([]byte(syncToken), &ct) != nil {
		return "", "", "", false
	}
	// An empty fingerprint means this is not one of our checkpoints; match the
	// loadResumeStates tolerance, which skips such tokens rather than failing.
	if ct.Fingerprint == "" {
		return "", "", "", false
	}
	return ct.Phase, ct.GrantPage, ct.SrcSyncID, true
}
