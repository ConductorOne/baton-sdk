package dotc1z

import (
	"errors"
)

// ErrSyncNotFinished is returned when a rollback targets a sync that has
// not finished. Expansion completeness for an in-progress sync lives in
// the entitlement graph persisted in the sync token, not in the grant
// rows. Deleting expansion output from such a sync would strand that
// graph: a later resume would treat the affected edges as already
// expanded and never regenerate the deleted grants. Pebble wraps
// pebble.ErrRollbackIncompleteCollection in it, and accepts an unfinished
// sync whose collection completed.
var ErrSyncNotFinished = errors.New("c1z: refusing to roll back expansion on an unfinished sync")

// ErrSyncNotExpanded is returned when a rollback targets a sync that never
// ran grant expansion: no grant in the sync carries Sources.
var ErrSyncNotExpanded = errors.New("c1z: sync did not run grant expansion; nothing to roll back")

// RollbackResult reports what a rollback changed, or — for a dry run —
// what it would change.
type RollbackResult struct {
	SyncID string
	// GrantsDeleted is the number of purely expander-derived grants
	// removed: their Sources carried only foreign entitlement keys, never
	// a self-source for their own entitlement.
	GrantsDeleted int
	// SourcesCleared is the number of surviving direct grants whose
	// Sources were reset to empty, restoring the pre-expansion shape.
	SourcesCleared int
	// SuspectConnectorSourced counts grants that had Sources but no
	// self-source AND lacked the GrantImmutable annotation that every
	// expander-created grant carries — a shape that looks connector-set
	// rather than expander-set. By default these are deleted (and counted
	// in GrantsDeleted); with WithPreserveSuspectGrants they are kept
	// instead (counted in SuspectPreserved, not GrantsDeleted). Either way
	// the count is surfaced so a real connector-sourced grant is visible.
	// Zero in the common case.
	SuspectConnectorSourced int
	// SuspectPreserved counts suspect grants kept rather than deleted
	// because WithPreserveSuspectGrants was set. Always 0 in the default
	// (delete) mode; a subset of SuspectConnectorSourced when set.
	SuspectPreserved int
	DryRun           bool
}

// rollbackConfig holds the tunable behavior of a rollback.
type rollbackConfig struct {
	preserveSuspect  bool
	rewriteSyncToken func(token string) (string, error)
}

// RollbackOption configures RollbackExpansion.
type RollbackOption func(*rollbackConfig)

// WithSyncTokenRewrite supplies how the sync's persisted token is rewritten
// once the grants are rolled back. The function receives the current token
// and returns the replacement; rollback writes the result verbatim. Without
// it, rollback clears the token to empty, which makes a resumed syncer seed a
// fresh run. A caller that wants to preserve the token's other state passes a
// rewrite that re-marks the sync for expansion instead. Ignored on a dry run.
func WithSyncTokenRewrite(fn func(token string) (string, error)) RollbackOption {
	return func(c *rollbackConfig) { c.rewriteSyncToken = fn }
}

// WithPreserveSuspectGrants makes rollback KEEP suspect connector-sourced
// grants (Sources present, no self-source, no GrantImmutable) instead of
// deleting them. Default OFF: a suspect grant is an expander artifact in
// the common case and is deleted, but because Grant.Sources is public
// connector data and replay runs an empty connector, a genuinely
// connector-set grant of that shape would be permanently dropped — this
// option trades a possibly-stale expander grant left in the output for
// never silently dropping real connector data.
func WithPreserveSuspectGrants() RollbackOption {
	return func(c *rollbackConfig) { c.preserveSuspect = true }
}
