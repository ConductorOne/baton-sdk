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

func NormalizeGrantCancelledReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return GrantCancelledDefaultReason
	}
	return reason
}
