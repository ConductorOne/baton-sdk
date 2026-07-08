package c1zsanitize

import "encoding/json"

// ResumePhase reads the sanitize resume phase encoded in a destination sync's
// raw sync_token. It returns the phase name (one of "resources",
// "entitlements", "grants", "assets"), the grant page token (meaningful only at
// the grants phase), the source sync id the token correlates to, and ok=false
// when the token is empty or not a c1zsanitize checkpoint. It does NOT validate
// the secret fingerprint and performs no I/O — it is a pure decoder over the
// token string a caller already holds (e.g. from c1zstore.SyncRun.SyncToken or
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
	// An empty fingerprint marks a token that is not one of our checkpoints, so
	// skip it — ResumePhase does not otherwise validate the fingerprint; matching
	// it against the secret is loadResumeStates' concern.
	if ct.Fingerprint == "" {
		return "", "", "", false
	}
	// A foreign token can carry an "fp" field yet name no phase we recognize;
	// such a token is not a resumable checkpoint, so report ok=false.
	switch ct.Phase {
	case phaseResources, phaseEntitlements, phaseGrants, phaseAssets:
	default:
		return "", "", "", false
	}
	return ct.Phase, ct.GrantPage, ct.SrcSyncID, true
}
