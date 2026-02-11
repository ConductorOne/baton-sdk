package metrics //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"fmt"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExtractFailureReason(t *testing.T) {
	tests := []struct {
		name              string
		err               error
		expectedCode      codes.Code
		expectedRateLimit bool
	}{
		{
			name:              "nil error returns Unknown",
			err:               nil,
			expectedCode:      codes.Unknown,
			expectedRateLimit: false,
		},
		{
			name:              "plain error returns Unknown",
			err:               fmt.Errorf("some error"),
			expectedCode:      codes.Unknown,
			expectedRateLimit: false,
		},
		{
			name:              "wrapped plain error returns Unknown",
			err:               fmt.Errorf("outer: %w", fmt.Errorf("inner error")),
			expectedCode:      codes.Unknown,
			expectedRateLimit: false,
		},
		{
			name:              "wrapped gRPC error preserves code",
			err:               fmt.Errorf("outer: %w", status.Error(codes.Unavailable, "service unavailable")),
			expectedCode:      codes.Unavailable,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC Unavailable without details",
			err:               status.Error(codes.Unavailable, "service unavailable"),
			expectedCode:      codes.Unavailable,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC PermissionDenied",
			err:               status.Error(codes.PermissionDenied, "access denied"),
			expectedCode:      codes.PermissionDenied,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC NotFound",
			err:               status.Error(codes.NotFound, "resource not found"),
			expectedCode:      codes.NotFound,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC Unauthenticated",
			err:               status.Error(codes.Unauthenticated, "invalid credentials"),
			expectedCode:      codes.Unauthenticated,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC DeadlineExceeded",
			err:               status.Error(codes.DeadlineExceeded, "timeout"),
			expectedCode:      codes.DeadlineExceeded,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC Unavailable with rate limit OVERLIMIT",
			err:               createRateLimitError(codes.Unavailable, v2.RateLimitDescription_STATUS_OVERLIMIT),
			expectedCode:      codes.Unavailable,
			expectedRateLimit: true,
		},
		{
			name:              "gRPC Unavailable with rate limit OK status",
			err:               createRateLimitError(codes.Unavailable, v2.RateLimitDescription_STATUS_OK),
			expectedCode:      codes.Unavailable,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC Unavailable with rate limit UNSPECIFIED status",
			err:               createRateLimitError(codes.Unavailable, v2.RateLimitDescription_STATUS_UNSPECIFIED),
			expectedCode:      codes.Unavailable,
			expectedRateLimit: false,
		},
		{
			name:              "gRPC Unavailable with rate limit ERROR status",
			err:               createRateLimitError(codes.Unavailable, v2.RateLimitDescription_STATUS_ERROR),
			expectedCode:      codes.Unavailable,
			expectedRateLimit: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason := extractFailureReason(tt.err)
			assert.Equal(t, tt.expectedCode, reason.GrpcCode, "unexpected gRPC code")
			assert.Equal(t, tt.expectedRateLimit, reason.IsRateLimit, "unexpected IsRateLimit value")
		})
	}
}

// createRateLimitError creates a gRPC status error with RateLimitDescription details.
func createRateLimitError(code codes.Code, rlStatus v2.RateLimitDescription_Status) error {
	st := status.New(code, "rate limited")
	rlDesc := v2.RateLimitDescription_builder{
		Status:    rlStatus,
		Limit:     100,
		Remaining: 0,
	}.Build()
	stWithDetails, err := st.WithDetails(rlDesc)
	if err != nil {
		// This should not happen in tests - fail loudly
		panic(fmt.Sprintf("failed to add rate limit details: %v", err))
	}
	return stWithDetails.Err()
}
