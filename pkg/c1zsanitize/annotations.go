package c1zsanitize

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// annotationHandler returns a sanitized replacement for the supplied
// annotation message. Implementations may register additional asset
// references via the supplied set so copyAssets can pick them up at
// end-of-sync. Implementations MUST NOT mutate the input.
type annotationHandler func(s *sanitizer, msg proto.Message, refs *assetRefSet) proto.Message

func defaultAnnotationHandlers() map[string]annotationHandler {
	return map[string]annotationHandler{
		typeURL(&v2.UserTrait{}):           handleUserTrait,
		typeURL(&v2.GroupTrait{}):          handleGroupTrait,
		typeURL(&v2.AppTrait{}):            handleAppTrait,
		typeURL(&v2.RoleTrait{}):           handleRoleTrait,
		typeURL(&v2.SecretTrait{}):         handleSecretTrait,
		typeURL(&v2.LicenseProfileTrait{}): handleLicenseProfileTrait,
		typeURL(&v2.ScopeBindingTrait{}):   handleScopeBindingTrait,

		// Non-trait annotations carrying graph topology / cross-references.
		// Preserved-and-sanitized rather than dropped so the sanitized c1z
		// stays a representative dataset for perf and graph testing.
		typeURL(&v2.GrantExpandable{}):      handleGrantExpandable,
		typeURL(&v2.GrantImmutable{}):       handleGrantImmutable,
		typeURL(&v2.EntitlementImmutable{}): handleEntitlementImmutable,
		typeURL(&v2.ExternalLink{}):         handleExternalLink,
		typeURL(&v2.ETag{}):                 handleETag,
		typeURL(&v2.ChildResourceType{}):    handleChildResourceType,
	}
}

// typeURL returns the canonical anypb type-URL for a proto message.
// It matches what anypb.New would set on Any.TypeUrl, so it works as
// a lookup key against the values returned by Any.GetTypeUrl().
func typeURL(m proto.Message) string {
	a, err := anypb.New(m)
	if err != nil {
		panic(err)
	}
	return a.GetTypeUrl()
}

// transformAnnotations walks the slice once, dispatching each entry on its
// Any type URL. Unknown types are dropped by default (the fail-closed posture)
// or passed through unchanged if the operator opted in via
// Options.AllowUnknownAnnotations=true. Per-entry outcomes are accumulated into
// per-type-URL counters (Sanitize emits one summary line at the end of the run
// via logDropSummary), and a single first-occurrence line is logged per distinct
// type_url so the signal — and, for failures, the actual error — is visible
// without the per-item spam these paths once produced (tens of millions of
// lines on a large file).
func (s *sanitizer) transformAnnotations(in []*anypb.Any, refs *assetRefSet) []*anypb.Any {
	if len(in) == 0 {
		return nil
	}
	out := make([]*anypb.Any, 0, len(in))
	for _, a := range in {
		if a == nil {
			continue
		}
		handler, ok := s.handlers[a.GetTypeUrl()]
		if !ok {
			// Drop is the default and the safe posture: an annotation type
			// with no handler has not been inspected, so passing it through
			// could leak un-sanitized customer data. Pass-through is opt-in
			// via Options.AllowUnknownAnnotations, for development only.
			if s.dropUnknownAnnotations {
				if s.recordAnnotation(s.droppedAnnotations, a.GetTypeUrl()) {
					s.log.Warn("c1zsanitize: dropping unknown annotation type (first occurrence; counted thereafter)",
						zap.String("type_url", a.GetTypeUrl()))
				}
				continue
			}
			if s.recordAnnotation(s.passedAnnotations, a.GetTypeUrl()) {
				s.log.Warn("c1zsanitize: passing unknown annotation through unchanged (first occurrence; counted thereafter)",
					zap.String("type_url", a.GetTypeUrl()))
			}
			out = append(out, a)
			continue
		}
		msg, err := a.UnmarshalNew()
		if err != nil {
			if s.recordAnnotation(s.failedAnnotations, a.GetTypeUrl()) {
				s.log.Warn("c1zsanitize: annotation transform failed (first occurrence; counted thereafter)",
					zap.String("type_url", a.GetTypeUrl()), zap.Error(err))
			}
			continue
		}
		sanitized := handler(s, msg, refs)
		if sanitized == nil {
			continue
		}
		repacked, err := anypb.New(sanitized)
		if err != nil {
			if s.recordAnnotation(s.failedAnnotations, a.GetTypeUrl()) {
				s.log.Warn("c1zsanitize: annotation transform failed (first occurrence; counted thereafter)",
					zap.String("type_url", a.GetTypeUrl()), zap.Error(err))
			}
			continue
		}
		out = append(out, repacked)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
