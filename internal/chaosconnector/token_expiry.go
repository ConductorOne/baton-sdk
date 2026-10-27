package chaosconnector

import (
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

// GraphExpiredTokenMessage is the text Microsoft Graph returns, as the
// SDK's HTTP wrapper surfaces it, when a pagination token has aged out.
const GraphExpiredTokenMessage = "Request failed with status 400: The specified page token value has expired and can no longer be included in your request."

// TokenExpiry is the Graph-mode pagination model: a page token issued by
// one response is rejected when a later request presents it more than
// Lifetime after it was issued, measured on Now. Untranslated, the
// rejection is the provider error as a connector without the translation
// would surface it: InvalidArgument with GraphExpiredTokenMessage.
// Translated, it is pagination.NewRestartActionError("", message). Tokens
// the run never saw issued (the root "" and anything unknown) never expire.
type TokenExpiry struct {
	Lifetime  time.Duration
	Translate bool
	// Defaults to time.Now. Tests supply a clock they advance per call.
	Now func() time.Time
}

type tokenExpiryState struct {
	TokenExpiry
	mu     sync.Mutex
	issued map[string]time.Time
	// Rejections by page token, for the oracle.
	rejected map[string]int
}

func newTokenExpiryState(expiry TokenExpiry) *tokenExpiryState {
	if expiry.Now == nil {
		expiry.Now = time.Now
	}
	return &tokenExpiryState{TokenExpiry: expiry, issued: make(map[string]time.Time), rejected: make(map[string]int)}
}

func (s *tokenExpiryState) record(response proto.Message) {
	if s == nil || response == nil {
		return
	}
	token := firstStringField(response.ProtoReflect(), "next_page_token")
	if token == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.issued[token] = s.Now()
}

func (s *tokenExpiryState) check(op Operation) error {
	if s == nil || op.PageToken == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	issued, ok := s.issued[op.PageToken]
	if !ok || s.Now().Sub(issued) <= s.Lifetime {
		return nil
	}
	s.rejected[op.PageToken]++
	if s.Translate {
		return pagination.NewRestartActionError("", GraphExpiredTokenMessage)
	}
	return status.Error(codes.InvalidArgument, GraphExpiredTokenMessage)
}

// Rejections returns how many requests each token was refused for.
func (s *tokenExpiryState) Rejections() map[string]int {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.rejected))
	for token, count := range s.rejected {
		out[token] = count
	}
	return out
}

// SetTokenExpiry arms Graph-mode token expiry for every list call the run's
// fault wrapper sees. Call before the first request.
func (r *Run) SetTokenExpiry(expiry TokenExpiry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tokenExpiry = newTokenExpiryState(expiry)
}

// TokenRejections reports Graph-mode refusals by token; nil when the mode
// is off.
func (r *Run) TokenRejections() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tokenExpiry.Rejections()
}

func (r *Run) expiry() *tokenExpiryState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tokenExpiry
}

func restartActionError(effect Effect) error {
	err := pagination.NewRestartActionError(effect.RestartToken, effect.Message)
	if effect.Code == codes.OK || effect.Code == codes.FailedPrecondition {
		return err
	}
	st := status.New(effect.Code, err.Error())
	withDetails, detailErr := st.WithDetails(v2.RestartAction_builder{RestartPageToken: effect.RestartToken, Reason: effect.Message}.Build())
	if detailErr != nil {
		return st.Err()
	}
	return withDetails.Err()
}
