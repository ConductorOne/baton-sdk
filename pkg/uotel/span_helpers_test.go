package uotel

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsExpectedError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, true},
		{"context canceled", context.Canceled, true},
		{"context deadline exceeded", context.DeadlineExceeded, true},
		{"wrapped context canceled", fmt.Errorf("wrapped: %w", context.Canceled), true},
		{"grpc canceled", status.Error(codes.Canceled, "canceled"), true},
		{"grpc deadline exceeded", status.Error(codes.DeadlineExceeded, "deadline"), true},
		{"generic error", errors.New("something broke"), false},
		{"grpc not found", status.Error(codes.NotFound, "not found"), false},
		{"grpc internal", status.Error(codes.Internal, "internal"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsExpectedError(tt.err)
			if got != tt.expected {
				t.Errorf("IsExpectedError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}
