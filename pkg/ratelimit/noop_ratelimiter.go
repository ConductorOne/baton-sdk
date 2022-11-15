package ratelimit

import (
	"context"

	v1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type NoOpRateLimiter struct{}

func (r *NoOpRateLimiter) Do(ctx context.Context, req *v1.DoRequest) (*v1.DoResponse, error) {
	return &v1.DoResponse{
		RequestToken: req.RequestToken,
		Description: &v1.RateLimitDescription{
			Status: v1.RateLimitDescription_EMPTY,
		},
	}, nil
}

func (r *NoOpRateLimiter) Report(ctx context.Context, req *v1.ReportRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
