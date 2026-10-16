package ugrpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type staticTokenError struct {
	err error
}

func (s staticTokenError) Token() (*oauth2.Token, error) {
	return nil, s.err
}

func TestGetRequestMetadataKeepsRestrictedCodesOffTheControlPlane(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "certificate failure",
			err:  status.Error(codes.FailedPrecondition, "tls certificate verification failed: example.com/auth/v1/token"),
			want: codes.Unauthenticated,
		},
		{
			name: "nxdomain",
			err:  status.Error(codes.InvalidArgument, "dns lookup failed: NXDOMAIN: example.com/auth/v1/token"),
			want: codes.Unavailable,
		},
		{
			name: "connection refused",
			err:  status.Error(codes.Unavailable, "connection refused: example.com/auth/v1/token"),
			want: codes.Unavailable,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &c1CredentialProvider{tokenSource: staticTokenError{err: tt.err}}
			_, err := p.GetRequestMetadata(t.Context(), "https://example.com")
			require.Equal(t, tt.want, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), status.Convert(tt.err).Message())
		})
	}
}
