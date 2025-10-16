package ratelimit

import (
	"context"

	v1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
)

type NoOpRateLimiter struct{}

func (r *NoOpRateLimiter) Do(ctx context.Context, req *v1.DoRequest) (*v1.DoResponse, error) {
	response := &v1.DoResponse{}
	response.SetRequestToken(req.GetRequestToken())
	desc := &v1.RateLimitDescription{}
	desc.SetStatus(v1.RateLimitDescription_STATUS_EMPTY)
	response.SetDescription(desc)
	return response, nil
}

func (r *NoOpRateLimiter) Report(ctx context.Context, req *v1.ReportRequest) (*v1.ReportResponse, error) {
	return &v1.ReportResponse{}, nil
}
