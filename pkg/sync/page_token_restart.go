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

var ErrPageTokenRestartLimit = errors.New("provider pagination restart limit reached")

type providerPageError struct{ cause error }

func (e providerPageError) Error() string { return e.cause.Error() }
func (e providerPageError) Unwrap() error { return e.cause }

type pageTokenRestartError struct {
	cause error
	code  codes.Code
}

func (e pageTokenRestartError) Error() string { return e.cause.Error() }
func (e pageTokenRestartError) Unwrap() error { return e.cause }
func (e pageTokenRestartError) GRPCStatus() *status.Status {
	return status.New(e.code, e.cause.Error())
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
		return "", pageTokenRestartError{cause: fmt.Errorf("%w (%d restarts): %w", ErrPageTokenRestartLimit, p.limit, cause), code: codes.FailedPrecondition}
	}
	// The generation separates recovery identities from arbitrary opaque provider tokens.
	return encodeRestartPageToken(action.pageTokenRestarts+1, rand.Text(), ""), nil
}

func logPageTokenRestart(ctx context.Context, action *Action, cause error) {
	ctxzap.Extract(ctx).Warn("sync: restarting provider pagination",
		zap.Object("action", action), zap.Uint32("page_token_restarts", action.pageTokenRestarts+1), zap.Error(cause))
}

// WithPageTokenRestarts restarts matching expired provider cursors at most limit
// times per action. Collected records are retained. Recovery checkpoints require
// this SDK version or newer to resume. A zero limit or nil classifier disables new
// restarts, but existing recovery cursors remain readable.
// isExpired may be called concurrently by sync workers.
func WithPageTokenRestarts(limit uint32, isExpired func(error) bool) SyncOpt {
	return func(s *syncer) {
		s.cfg.pageTokenRestartPolicy = &pageTokenRestartPolicy{limit: limit, isExpired: isExpired}
	}
}
