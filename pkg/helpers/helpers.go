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

func ExtractRateLimitData(header *http.Header) (*v2.RateLimitDescription, error) {
	if header == nil {
		return nil, nil
	}

	var l int64
	var err error
	limit := header.Get("X-Ratelimit-Limit")
	if limit != "" {
		l, err = strconv.ParseInt(limit, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	var r int64
	remaining := header.Get("X-Ratelimit-Remaining")
	if remaining != "" {
		r, err = strconv.ParseInt(remaining, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	var resetAt time.Time
	reset := header.Get("X-Ratelimit-Reset")
	if reset != "" {
		res, err := strconv.ParseInt(reset, 10, 64)
		if err != nil {
			return nil, err
		}

		resetAt = time.Now().Add(time.Second * time.Duration(res))
	}

	return &v2.RateLimitDescription{
		Limit:     l,
		Remaining: r,
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
