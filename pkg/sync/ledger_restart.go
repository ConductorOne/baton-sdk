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

// ErrRestartLimitExceeded marks a sync that cannot finish: one action
// restarted more times than the per-action limit allows, or the sync
// restarted more pages than its per-sync limit. pkg/tasks/c1api classifies
// it non-retryable; a resume would call the connector once and fail the same
// way. The artifact is kept (RFC 0009).
var ErrRestartLimitExceeded = c1zstore.ErrRestartLimitExceeded

// ErrRestartRejected is a connector error carrying a RestartAction detail
// from a call that cannot honor it: a status code the retry or warning
// layers own, a call other than the action's own list call, or a spawned
// cursor. The detail is neither honored nor dropped; the sync fails.
var ErrRestartRejected = errors.New("connector requested a restart where it cannot be honored")

const (
	ledgerActionRestarts     = c1zstore.LedgerActionRestarts
	ledgerRestartsPrefix     = ledgerActionRestarts + "."
	defaultRestartsPerAction = 5
	defaultRestartsPerSync   = 100
)

// WithRestartLimits bounds connector-signaled restarts: perAction is how
// many times one action may restart within a sync before the next request
// fails the sync; perSync is the limit for the whole sync. Zero leaves the
// default in place.
func WithRestartLimits(perAction, perSync uint64) SyncOpt {
	return func(s *syncer) {
		if perAction > 0 {
			s.cfg.restartLimitPerAction = perAction
		}
		if perSync > 0 {
			s.cfg.restartLimitPerSync = perSync
		}
	}
}

func (s *syncer) restartLimits() c1zstore.RestartLimits {
	limits := c1zstore.RestartLimits{PerAction: s.cfg.restartLimitPerAction, PerSync: s.cfg.restartLimitPerSync}
	if limits.PerAction == 0 {
		limits.PerAction = defaultRestartsPerAction
	}
	if limits.PerSync == 0 {
		limits.PerSync = defaultRestartsPerSync
	}
	return limits
}

// The actions whose connector call pages on the action's own token. Roots
// of the per-type ops (no resource type) read the store, or, for static
// entitlements, paginate a nested ListResourceTypes on a token of their
// own, so a restart token means nothing to them.
func actionHonorsRestart(action *Action) bool {
	switch action.Op {
	case SyncResourceTypesOp:
		return true
	case SyncResourcesOp, SyncEntitlementsOp, SyncGrantsOp, SyncStaticEntitlementsOp:
		return action.ResourceTypeID != ""
	default:
		return false
	}
}

func codeHonorsRestart(code codes.Code) bool {
	return code == codes.FailedPrecondition || code == codes.Aborted
}

// actionListCallError marks an error as the return of the action's own
// paginated list call. A handler makes other connector calls too (the
// grants handler fetches related resources with GetResource); a detail on
// one of those names a token the action never paged on.
type actionListCallError struct{ err error }

func (e *actionListCallError) Error() string { return e.err.Error() }
func (e *actionListCallError) Unwrap() error { return e.err }

func markActionListCall(err error) error {
	if err == nil {
		return nil
	}
	return &actionListCallError{err: err}
}

func fromActionListCall(err error) bool {
	var marked *actionListCallError
	return errors.As(err, &marked)
}

// rejectedRestartError names the cell and wraps the connector's
// error. Its own status is Internal so the connector's code does not reach
// isWarning (NotFound finishes the action with a warning) or
// retry.ShouldWaitAndRetry (Unavailable, DeadlineExceeded) in the callers
// of invokeActionPage; the connector's status is still reachable through
// Unwrap.
type rejectedRestartError struct {
	cell  string
	cause error
}

func (e *rejectedRestartError) Error() string {
	return ErrRestartRejected.Error() + ": " + e.cell + ": " + e.cause.Error()
}

func (e *rejectedRestartError) Unwrap() []error {
	return []error{ErrRestartRejected, e.cause}
}

func (e *rejectedRestartError) GRPCStatus() *status.Status {
	return status.New(codes.Internal, e.Error())
}

// classifyRestartRequest returns the RestartAction detail err
// carries, whether one is present, and, when this action cannot honor it,
// an error that names the cell and wraps err.
func classifyRestartRequest(action *Action, err error) (*v2.RestartAction, bool, error) {
	detail, present := pagination.RestartActionFromError(err)
	if !present {
		return nil, false, nil
	}
	code := status.Code(err)
	ownCall := fromActionListCall(err)
	if !codeHonorsRestart(code) || !actionHonorsRestart(action) || action.Spawned || !ownCall {
		cell := fmt.Sprintf("op=%s resource_type=%q code=%s spawned=%t own_list_call=%t", action.Op.String(), action.ResourceTypeID, code.String(), action.Spawned, ownCall)
		return detail, true, &rejectedRestartError{cell: cell, cause: err}
	}
	return detail, true, nil
}

type restartRequest struct {
	detail *v2.RestartAction
	cause  error
}

// stageActionRestart turns the connector's error into the page: a restarted
// row at the expired token whose continuation is the restart token, the
// restart counters, and the replay block. The handler staged nothing
// before the connector call failed, and the engine refuses a restart page
// that carries records, children, or claims.
func (s *syncer) stageActionRestart(invocation *ledgerInvocation, page *ledgerPage, action *Action, detail *v2.RestartAction, cause error) error {
	if page.transitions != 0 {
		return errors.New("restart followed an action transition")
	}
	if err := invocation.stage(action, detail.GetRestartPageToken(), nil); err != nil {
		return err
	}
	page.row.Restarted = true
	invocation.children = nil
	invocation.afterCommit = nil
	if err := page.writer.SetRestart(s.restartLimits()); err != nil {
		return err
	}
	if page.observations.Counters == nil {
		page.observations.Counters = make(map[string]uint64)
	}
	page.observations.Counters[ledgerActionRestarts]++
	page.observations.Counters[ledgerRestartsPrefix+action.Op.String()]++
	page.observations.Flags |= ingestQualityReasonActionRestarted
	if err := page.setFact(ledgerFactIngestKnown); err != nil {
		return err
	}
	if err := page.setFact(ledgerFactIngestBlocked); err != nil {
		return err
	}
	invocation.restart = &restartRequest{detail: detail, cause: cause}
	op := action.Op
	invocation.afterCommit = append(invocation.afterCommit, func() {
		s.ingestFilterStats.blockReplay(ingestQualityReasonActionRestarted)
		s.run.recordRestart(op)
	})
	return nil
}

// RestartLimitError is the sync's terminal error when a restart
// was refused by a limit. Cause is the connector's error for the page that
// tripped it; Limit carries the pending item's restart list.
type RestartLimitError struct {
	Action Action
	Limit  *c1zstore.RestartLimitError
	Cause  error
}

func (e *RestartLimitError) Error() string {
	return fmt.Sprintf("%v (%s %s/%s): %v", e.Limit, e.Action.Op.String(), e.Action.ResourceTypeID, e.Action.ResourceID, e.Cause)
}

// Unwrap exposes the sentinel for errors.Is, the limit detail for errors.As,
// and the connector's status for status.FromError.
func (e *RestartLimitError) Unwrap() []error {
	return []error{ErrRestartLimitExceeded, e.Limit, e.Cause}
}

// wrapRestartCommitError maps the engine's refusal of a restart page to
// the sync's terminal error; any other commit failure is returned as is.
func wrapRestartCommitError(action *Action, request *restartRequest, err error) error {
	if request == nil || err == nil {
		return err
	}
	var limitErr *c1zstore.RestartLimitError
	if !errors.As(err, &limitErr) {
		return err
	}
	return &RestartLimitError{Action: *action, Limit: limitErr, Cause: request.cause}
}

func restartLogFields(action *Action, detail *v2.RestartAction) []zap.Field {
	return []zap.Field{
		zap.Object("action", *action),
		zap.Uint64("work_id", action.WorkID),
		zap.Uint64("work_revision", action.WorkRevision),
		zap.Bool("restart_from_first_page", detail.GetRestartPageToken() == ""),
		zap.String("reason", detail.GetReason()),
	}
}
