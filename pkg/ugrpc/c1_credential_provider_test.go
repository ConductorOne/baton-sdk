package ugrpc

import (
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestTokenStatusErrorClassification pins the mapping from token-endpoint HTTP
// statuses to gRPC codes. Transient server/throttling responses must be
// retryable (Unavailable) so a momentary token-service outage at startup does
// not permanently skip the Hello, while genuine credential problems stay
// non-retryable (Unauthenticated) so misconfigured connectors fail fast.
func TestTokenStatusErrorClassification(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		want       codes.Code
	}{
		// The production failure that motivated this: a transient 503 from the
		// token endpoint must be retryable, not a fatal auth error.
		{"503-service-unavailable", http.StatusServiceUnavailable, codes.Unavailable},
		{"500-internal", http.StatusInternalServerError, codes.Unavailable},
		{"502-bad-gateway", http.StatusBadGateway, codes.Unavailable},
		{"504-gateway-timeout", http.StatusGatewayTimeout, codes.Unavailable},
		{"429-too-many-requests", http.StatusTooManyRequests, codes.Unavailable},
		{"408-request-timeout", http.StatusRequestTimeout, codes.Unavailable},
		// Genuine credential / request problems remain fatal.
		{"401-unauthorized", http.StatusUnauthorized, codes.Unauthenticated},
		{"403-forbidden", http.StatusForbidden, codes.Unauthenticated},
		{"400-bad-request", http.StatusBadRequest, codes.Unauthenticated},
		{"404-not-found", http.StatusNotFound, codes.Unauthenticated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tokenStatusError(tc.statusCode, http.StatusText(tc.statusCode))
			if got := status.Code(err); got != tc.want {
				t.Fatalf("tokenStatusError(%d) code = %s, want %s", tc.statusCode, got, tc.want)
			}
		})
	}
}
