package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const paginationRestartPrefix = "baton-sdk:page-restart:"

// ErrPageTokenRestartLimit reports an expiry after an action spent its
// WithPageTokenRestarts budget. The error carries codes.FailedPrecondition and
// wraps the connector's error. IsSyncPreservable reports true for it and
// ShouldDiscardSyncArtifact false, but the checkpoint keeps the spent budget and
// the expired cursor, so every resume of that sync fails again: a host that
// resumes preserved syncs must cap those resumes and then start a new sync.
var ErrPageTokenRestartLimit = errors.New("provider pagination restart limit reached")

type providerPageError struct{ cause error }

func (e providerPageError) Error() string { return e.cause.Error() }
func (e providerPageError) Unwrap() error { return e.cause }

// A providerPageError whose cause carries a gRPC status. Without GRPCStatus,
// status.FromError would find the cause through Unwrap and replace its message
// with Error(), which carries the "rpc error: code = ... desc = " prefix. Causes
// without a status keep the plain type: a nil GRPCStatus would stop errors.As
// before a status later in a joined error.
type providerStatusPageError struct {
	providerPageError
	status *status.Status
}

func (e providerStatusPageError) Unwrap() error              { return e.providerPageError }
func (e providerStatusPageError) GRPCStatus() *status.Status { return e.status }

func newProviderPageError(err error) error {
	page := providerPageError{cause: err}
	if s, ok := status.FromError(err); ok {
		return providerStatusPageError{providerPageError: page, status: s}
	}
	return page
}

type pageTokenRestartLimitError struct{ cause error }

func (e pageTokenRestartLimitError) Error() string { return e.cause.Error() }
func (e pageTokenRestartLimitError) Unwrap() error { return e.cause }
func (e pageTokenRestartLimitError) GRPCStatus() *status.Status {
	return status.New(codes.FailedPrecondition, e.cause.Error())
}

type pageTokenRestartPolicy struct {
	limit     uint32
	isExpired func(error) bool
}

func encodeRestartPageToken(restarts uint32, generation, token string) string {
	return paginationRestartPrefix + "1:" + strconv.FormatUint(uint64(restarts), 10) + ":" + generation + ":" + base64.RawURLEncoding.EncodeToString([]byte(token))
}

func providerPageAction(action *Action) (*Action, error) {
	if !action.PageTokenRestarted {
		return action, nil
	}
	encoded, found := strings.CutPrefix(action.PageToken, paginationRestartPrefix)
	if !found {
		return nil, errors.New("invalid pagination restart cursor")
	}
	version, encoded, ok := strings.Cut(encoded, ":")
	if !ok || version != "1" {
		return nil, errors.New("unsupported pagination restart cursor version")
	}
	count, encoded, ok := strings.Cut(encoded, ":")
	if !ok {
		return nil, errors.New("invalid pagination restart cursor")
	}
	restarts, err := strconv.ParseUint(count, 10, 32)
	if err != nil || restarts == 0 {
		return nil, errors.New("invalid pagination restart count")
	}
	generation, encoded, ok := strings.Cut(encoded, ":")
	if !ok || len(generation) < 26 || len(generation) > 128 {
		return nil, errors.New("invalid pagination restart generation")
	}
	for _, b := range []byte(generation) {
		if (b < 'a' || b > 'z') && (b < 'A' || b > 'Z') && (b < '0' || b > '9') {
			return nil, errors.New("invalid pagination restart generation")
		}
	}
	token, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("invalid pagination restart provider cursor")
	}
	request := *action
	request.PageToken = string(token)
	request.pageTokenRestarts = uint32(restarts)
	request.pageTokenRestartGeneration = generation
	return &request, nil
}

func providerPaginatedAction(action *Action) bool {
	if action.Spawned {
		return false
	}
	switch action.Op {
	case SyncResourceTypesOp:
		return true
	case SyncResourcesOp, SyncEntitlementsOp, SyncStaticEntitlementsOp, SyncGrantsOp:
		return action.ResourceTypeID != ""
	default:
		return false
	}
}

func (p *pageTokenRestartPolicy) next(ctx context.Context, action *Action, cause error) (string, error) {
	if p == nil || p.limit == 0 || p.isExpired == nil || ctx.Err() != nil || action.PageToken == "" || !providerPaginatedAction(action) || !p.isExpired(cause) {
		return "", cause
	}
	var providerErr providerPageError
	if !errors.As(cause, &providerErr) {
		return "", cause
	}
	if action.pageTokenRestarts >= p.limit {
		return "", pageTokenRestartLimitError{cause: fmt.Errorf("%w (%d restarts): %w", ErrPageTokenRestartLimit, p.limit, cause)}
	}
	// The generation separates recovery identities from arbitrary opaque provider tokens.
	return encodeRestartPageToken(action.pageTokenRestarts+1, rand.Text(), ""), nil
}

func logPageTokenRestart(ctx context.Context, action *Action, cause error) {
	ctxzap.Extract(ctx).Warn("sync: restarting provider pagination",
		zap.Object("action", action), zap.Uint32("page_token_restarts", action.pageTokenRestarts+1), zap.Error(cause))
}

// WithPageTokenRestarts restarts an action's provider listing from its first page
// when isExpired matches the connector's error, at most limit times per action per
// sync. Eligible calls are ListResourceTypes, ListResources, ListEntitlements,
// ListStaticEntitlements and ListGrants sent with a nonempty page token, except
// cursors a connector spawned with the EnqueuePageTokens annotation. Store and SDK
// errors never restart. The budget is checkpointed, so a resume does not refill
// it; once it is spent, the next expiry fails with ErrPageTokenRestartLimit.
// Collected records are retained. A zero limit or nil classifier disables new
// restarts, but existing recovery cursors remain readable. isExpired may be called
// concurrently by sync workers.
//
// Recovery checkpoints require this SDK version or newer. An older SDK drops a
// SQLite checkpoint's recovery action stack and collects again. On Pebble, from
// an action's first restart until collection finishes, an older SDK can neither
// resume the file nor start a new sync over it; finish the sync with a supporting
// SDK or discard the file.
func WithPageTokenRestarts(limit uint32, isExpired func(error) bool) SyncOpt {
	return func(s *syncer) {
		s.cfg.pageTokenRestartPolicy = &pageTokenRestartPolicy{limit: limit, isExpired: isExpired}
	}
}
