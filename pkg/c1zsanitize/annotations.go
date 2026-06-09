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

// transformAnnotations walks the slice once, dispatching each entry
// on its Any type URL. Unknown types are dropped by default or passed
// through unchanged if the operator opted in via
// Options.AllowUnknownAnnotations=true.
//
// The dropped / passed counts and a per-type-URL breakdown are
// accumulated on the sanitizer struct and surfaced in the per-phase
// progress + end-of-run summary log lines. Per-annotation logging is
// intentionally absent: a real sanitize run iterates tens of millions
// of records each carrying multiple annotations, so a one-line-per-
// annotation log produced millions of entries with no progress signal.
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
				s.droppedUnknownAnnotations++
				s.droppedUnknownAnnotationTypes[a.GetTypeUrl()]++
				continue
			}
			s.passedUnknownAnnotations++
			s.passedUnknownAnnotationTypes[a.GetTypeUrl()]++
			out = append(out, a)
			continue
		}
		msg, err := a.UnmarshalNew()
		if err != nil {
			s.log.Warn("c1zsanitize: failed to unmarshal annotation; dropping",
				zap.String("type_url", a.GetTypeUrl()), zap.Error(err))
			continue
		}
		sanitized := handler(s, msg, refs)
		if sanitized == nil {
			continue
		}
		repacked, err := anypb.New(sanitized)
		if err != nil {
			s.log.Warn("c1zsanitize: failed to repack annotation; dropping",
				zap.String("type_url", a.GetTypeUrl()), zap.Error(err))
			continue
		}
		out = append(out, repacked)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
