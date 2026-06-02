package grant

import (
	"errors"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	GrantCancelledDefaultReason = "Grant cancelled by connector policy."

	GrantCancelledErrorInfoDomain            = "conductorone.com/baton-sdk"
	GrantCancelledErrorInfoReason            = "GRANT_CANCELLED"
	GrantCancelledErrorInfoReasonMetadataKey = "reason"
)

func GrantCancelledReasonFromError(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	var grantCancelled *ErrGrantCancelled
	if errors.As(err, &grantCancelled) {
		return NormalizeGrantCancelledReason(grantCancelled.Error()), true
	}

	st, ok := status.FromError(err)
	if !ok {
		return "", false
	}

	return GrantCancelledReasonFromStatus(st.Proto())
}

func GrantCancelledReasonFromStatus(st *rpcstatus.Status) (string, bool) {
	if st == nil {
		return "", false
	}

	for _, detail := range st.GetDetails() {
		info := &errdetails.ErrorInfo{}
		if err := detail.UnmarshalTo(info); err != nil {
			continue
		}
		if info.GetDomain() != GrantCancelledErrorInfoDomain || info.GetReason() != GrantCancelledErrorInfoReason {
			continue
		}
		reason := info.GetMetadata()[GrantCancelledErrorInfoReasonMetadataKey]
		if reason == "" {
			reason = st.GetMessage()
		}
		return NormalizeGrantCancelledReason(reason), true
	}

	return "", false
}

func GrantCancelledErrorInfo(reason string) *errdetails.ErrorInfo {
	return &errdetails.ErrorInfo{
		Domain: GrantCancelledErrorInfoDomain,
		Reason: GrantCancelledErrorInfoReason,
		Metadata: map[string]string{
			GrantCancelledErrorInfoReasonMetadataKey: NormalizeGrantCancelledReason(reason),
		},
	}
}

func StatusWithGrantCancelledErrorInfo(st *status.Status, reason string) (*status.Status, error) {
	if st == nil {
		st = status.New(codes.Unknown, NormalizeGrantCancelledReason(reason))
	}
	return st.WithDetails(GrantCancelledErrorInfo(reason))
}

// StatusForGrantCancelledError translates a grant-cancellation Go error into the
// on-the-wire status that carries the marker, reporting whether err (or anything
// it wraps) was a grant cancellation. It is the single encoder shared by every
// connector server boundary (the lambda transport and the subprocess connector
// transport) so the marker is emitted identically regardless of how a connector
// is deployed.
func StatusForGrantCancelledError(err error) (*status.Status, bool) {
	reason, ok := GrantCancelledReasonFromError(err)
	if !ok {
		return nil, false
	}
	st, detailErr := StatusWithGrantCancelledErrorInfo(nil, reason)
	if detailErr != nil {
		// WithDetails only fails for codes.OK and this status is codes.Unknown,
		// so this is practically unreachable. Degrade to a detail-less status
		// rather than dropping the failure entirely.
		return status.New(codes.Unknown, reason), true
	}
	return st, true
}

func NormalizeGrantCancelledReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return GrantCancelledDefaultReason
	}
	return reason
}
