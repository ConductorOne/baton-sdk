package helpers

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func SplitFullName(name string) (string, string) {
	names := strings.SplitN(name, " ", 2)
	var firstName, lastName string

	switch len(names) {
	case 1:
		firstName = names[0]
	case 2:
		firstName = names[0]
		lastName = names[1]
	}

	return firstName, lastName
}

var limitHeaders = []string{
	"X-Ratelimit-Limit",
	"Ratelimit-Limit",
	"X-RateLimit-Requests-Limit", // Linear uses a non-standard header
}

var remainingHeaders = []string{
	"X-Ratelimit-Remaining",
	"Ratelimit-Remaining",
	"X-RateLimit-Requests-Remaining", // Linear uses a non-standard header
}

var resetAtHeaders = []string{
	"X-Ratelimit-Reset",
	"Ratelimit-Reset",
	"X-RateLimit-Requests-Reset", // Linear uses a non-standard header
	"Retry-After",                // Often returned with 429
}

func ExtractRateLimitData(statusCode int, header *http.Header) (*v2.RateLimitDescription, error) {
	if header == nil {
		return nil, nil
	}

	var rlstatus v2.RateLimitDescription_Status

	var limit int64
	var err error
	for _, limitHeader := range limitHeaders {
		limitStr := header.Get(limitHeader)
		if limitStr != "" {
			limit, err = strconv.ParseInt(limitStr, 10, 64)
			if err != nil {
				return nil, err
			}
			break
		}
	}

	var remaining int64
	for _, remainingHeader := range remainingHeaders {
		remainingStr := header.Get(remainingHeader)
		if remainingStr != "" {
			remaining, err = strconv.ParseInt(remainingStr, 10, 64)
			if err != nil {
				return nil, err
			}
			break
		}
	}
	if remaining > 0 {
		rlstatus = v2.RateLimitDescription_STATUS_OK
	}

	var resetAt time.Time
	for _, resetAtHeader := range resetAtHeaders {
		resetAtStr := header.Get(resetAtHeader)
		if resetAtStr != "" {
			res, err := strconv.ParseInt(resetAtStr, 10, 64)
			if err != nil {
				return nil, err
			}
			resetAt = time.Now().Add(time.Second * time.Duration(res))
			break
		}
	}

	if statusCode == http.StatusTooManyRequests {
		rlstatus = v2.RateLimitDescription_STATUS_OVERLIMIT
		remaining = 0
	}

	// If we didn't get any rate limit headers and status code is 429, return some sane defaults
	if remaining == 0 && resetAt.IsZero() && rlstatus == v2.RateLimitDescription_STATUS_OVERLIMIT {
		limit = 1
		resetAt = time.Now().Add(time.Second * 60)
	}

	return &v2.RateLimitDescription{
		Status:    rlstatus,
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   timestamppb.New(resetAt),
	}, nil
}

func IsJSONContentType(contentType string) bool {
	if !strings.HasPrefix(contentType, "application") {
		return false
	}

	if !strings.Contains(contentType, "json") {
		return false
	}

	return true
}

var xmlContentTypes []string = []string{
	"text/xml",
	"application/xml",
}

func IsXMLContentType(contentType string) bool {
	// there are some janky APIs out there
	normalizedContentType := strings.TrimSpace(strings.ToLower(contentType))

	for _, xmlContentType := range xmlContentTypes {
		if strings.HasPrefix(normalizedContentType, xmlContentType) {
			return true
		}
	}
	return false
}
