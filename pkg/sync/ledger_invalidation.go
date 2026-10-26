package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"errors"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

// ErrActionInvalidationCapExceeded marks a sync that cannot finish: one
// action's page token expired more times than the per-item cap allows, or
// the sync restarted more pages than its budget. pkg/tasks/c1api classifies
// it non-retryable; a resume would call the connector once and fail the same
// way. The artifact is kept (RFC 0009).
var ErrActionInvalidationCapExceeded = errors.New("action invalidation cap exceeded")

// ErrActionInvalidationUnsupported marks a sync on a store with no page
// ledger (a sqlite c1z) whose connector asked for a restart. There is
// nothing to commit a restart against, so the sync cannot finish;
// pkg/tasks/c1api classifies it non-retryable like the cap.
var ErrActionInvalidationUnsupported = errors.New("connector invalidated a page token but this store cannot restart actions")

// ErrActionInvalidationMisclassified is a connector error carrying an
// InvalidateAction detail on a call that cannot honor it: a status code the
// retry or warning layers own, a non-paginated or local operation, or a
// spawned cursor (stage 1). The detail is neither honored nor dropped.
var ErrActionInvalidationMisclassified = errors.New("connector returned InvalidateAction where it cannot be honored")

const (
	ledgerActionInvalidations  = c1zstore.LedgerActionInvalidations
	ledgerInvalidationsPrefix  = ledgerActionInvalidations + "."
	defaultInvalidationPerItem = 5
	defaultInvalidationPerSync = 100
)

// WithActionInvalidationCaps bounds connector-signaled restarts: perItem is
// how many times one action may restart within a sync before the next
// expiry fails the sync; perSync is the restart budget for the whole sync.
// Zero leaves the default in place.
func WithActionInvalidationCaps(perItem, perSync uint64) SyncOpt {
	return func(s *syncer) {
		if perItem > 0 {
			s.cfg.invalidationCapPerItem = perItem
		}
		if perSync > 0 {
			s.cfg.invalidationCapPerSync = perSync
		}
	}
}

func (s *syncer) restartCaps() c1zstore.RestartCaps {
	caps := c1zstore.RestartCaps{PerItem: s.cfg.invalidationCapPerItem, PerSync: s.cfg.invalidationCapPerSync}
	if caps.PerItem == 0 {
		caps.PerItem = defaultInvalidationPerItem
	}
	if caps.PerSync == 0 {
		caps.PerSync = defaultInvalidationPerSync
	}
	return caps
}

// The actions whose connector call pages on the action's own token. Roots
// of the per-type ops (no resource type) read the store, or, for static
// entitlements, paginate a nested ListResourceTypes on a token of their
// own, so a restart token means nothing to them.
func honorsActionInvalidation(action *Action) bool {
	switch action.Op {
	case SyncResourceTypesOp:
		return true
	case SyncResourcesOp, SyncEntitlementsOp, SyncGrantsOp, SyncStaticEntitlementsOp:
		return action.ResourceTypeID != ""
	default:
		return false
	}
}

func honorsInvalidationCode(code codes.Code) bool {
	return code == codes.FailedPrecondition || code == codes.Aborted
}

// classifyActionInvalidation returns the InvalidateAction detail err
// carries, whether one is present, and, when this action cannot honor it,
// an error that names the cell and wraps err.
func classifyActionInvalidation(action *Action, err error) (*v2.InvalidateAction, bool, error) {
	detail, present := pagination.InvalidateActionFromError(err)
	if !present {
		return nil, false, nil
	}
	code := status.Code(err)
	if !honorsInvalidationCode(code) || !honorsActionInvalidation(action) || action.Spawned {
		return detail, true, fmt.Errorf("%w: op=%s resource_type=%q code=%s spawned=%t: %w",
			ErrActionInvalidationMisclassified, action.Op.String(), action.ResourceTypeID, code.String(), action.Spawned, err)
	}
	return detail, true, nil
}

type actionInvalidation struct {
	detail *v2.InvalidateAction
	cause  error
}

// stageActionRestart turns the connector's error into the page: a restarted
// row at the expired token whose continuation is the restart token, the
// invalidation counters, and the replay block. The handler staged nothing
// before the connector call failed, and the engine refuses a restart page
// that carries records, children, or claims.
func (s *syncer) stageActionRestart(invocation *ledgerInvocation, page *ledgerPage, action *Action, detail *v2.InvalidateAction, cause error) error {
	if page.transitions != 0 {
		return errors.New("action invalidation followed an action transition")
	}
	if err := invocation.stage(action, detail.GetRestartPageToken(), nil); err != nil {
		return err
	}
	page.row.Restarted = true
	invocation.children = nil
	invocation.afterCommit = nil
	if err := page.writer.SetRestart(s.restartCaps()); err != nil {
		return err
	}
	if page.observations.Counters == nil {
		page.observations.Counters = make(map[string]uint64)
	}
	page.observations.Counters[ledgerActionInvalidations]++
	page.observations.Counters[ledgerInvalidationsPrefix+action.Op.String()]++
	page.observations.Flags |= ingestQualityReasonActionInvalidated
	if err := page.setFact(ledgerFactIngestKnown); err != nil {
		return err
	}
	if err := page.setFact(ledgerFactIngestBlocked); err != nil {
		return err
	}
	invocation.invalidation = &actionInvalidation{detail: detail, cause: cause}
	op := action.Op
	invocation.afterCommit = append(invocation.afterCommit, func() {
		s.ingestFilterStats.blockReplay(ingestQualityReasonActionInvalidated)
		s.run.recordActionInvalidation(op)
	})
	return nil
}

// ActionInvalidationCapError is the sync's terminal error when a restart
// was refused by a cap. Cause is the connector's error for the page that
// tripped it; Cap carries the pending item's invalidation list.
type ActionInvalidationCapError struct {
	Action Action
	Cap    *c1zstore.InvalidationCapError
	Cause  error
}

func (e *ActionInvalidationCapError) Error() string {
	return fmt.Sprintf("%v: %s %s/%s: %v: %v", ErrActionInvalidationCapExceeded, e.Action.Op.String(),
		e.Action.ResourceTypeID, e.Action.ResourceID, e.Cap, e.Cause)
}

// Unwrap exposes the sentinel for errors.Is, the cap detail for errors.As,
// and the connector's status for status.FromError.
func (e *ActionInvalidationCapError) Unwrap() []error {
	return []error{ErrActionInvalidationCapExceeded, e.Cap, e.Cause}
}

// wrapInvalidationCommitError maps the engine's refusal of a restart page to
// the sync's terminal error; any other commit failure is returned as is.
func wrapInvalidationCommitError(action *Action, invalidation *actionInvalidation, err error) error {
	if invalidation == nil || err == nil {
		return err
	}
	var capErr *c1zstore.InvalidationCapError
	if !errors.As(err, &capErr) {
		return err
	}
	return &ActionInvalidationCapError{Action: *action, Cap: capErr, Cause: invalidation.cause}
}

// wrapUnsupportedInvalidation is the legacy (non-ledgered) path's answer to
// the detail: the connector's error unchanged in code and text, joined with
// the sentinel so the task layer stops resuming. Errors without the detail
// pass through untouched.
func wrapUnsupportedInvalidation(action *Action, err error) error {
	if _, present := pagination.InvalidateActionFromError(err); !present {
		return err
	}
	return fmt.Errorf("%w: %s %s/%s: %w", ErrActionInvalidationUnsupported, action.Op.String(), action.ResourceTypeID, action.ResourceID, err)
}

func actionInvalidationLogFields(action *Action, detail *v2.InvalidateAction) []zap.Field {
	return []zap.Field{
		zap.Object("action", *action),
		zap.Uint64("work_id", action.WorkID),
		zap.Uint64("work_revision", action.WorkRevision),
		zap.Bool("restart_from_first_page", detail.GetRestartPageToken() == ""),
		zap.String("reason", detail.GetReason()),
	}
}
