package v2

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// validateUsageEvent validates through Event, which is where a consumer
// rejects the whole event on one failing value.
func validateUsageEvent(usageEvent *UsageEvent) error {
	return Event_builder{Id: "1", UsageEvent: usageEvent}.Build().ValidateAll()
}

func TestNetworkOriginValidation(t *testing.T) {
	tests := []struct {
		name    string
		network NetworkOrigin_builder
		wantErr bool
	}{
		{name: "unset fields", network: NetworkOrigin_builder{}},
		{name: "ipv4", network: NetworkOrigin_builder{IpAddress: "203.0.113.7"}},
		{name: "ipv6", network: NetworkOrigin_builder{IpAddress: "2001:db8::1"}},
		{name: "ip with port", network: NetworkOrigin_builder{IpAddress: "203.0.113.7:443"}, wantErr: true},
		{name: "ip list", network: NetworkOrigin_builder{IpAddress: "203.0.113.7, 198.51.100.1"}, wantErr: true},
		{name: "ip placeholder", network: NetworkOrigin_builder{IpAddress: "unknown"}, wantErr: true},
		{name: "country", network: NetworkOrigin_builder{CountryCode: "US"}},
		{name: "country lowercase", network: NetworkOrigin_builder{CountryCode: "us"}, wantErr: true},
		{name: "country alpha-3", network: NetworkOrigin_builder{CountryCode: "USA"}, wantErr: true},
		{name: "country digit", network: NetworkOrigin_builder{CountryCode: "1A"}, wantErr: true},
		{name: "subdivision", network: NetworkOrigin_builder{SubdivisionCode: "US-OR"}},
		{name: "subdivision three letters", network: NetworkOrigin_builder{SubdivisionCode: "GB-ENG"}},
		{name: "subdivision alphanumeric", network: NetworkOrigin_builder{SubdivisionCode: "FR-75C"}},
		{name: "subdivision without country", network: NetworkOrigin_builder{SubdivisionCode: "OR"}, wantErr: true},
		{name: "subdivision lowercase", network: NetworkOrigin_builder{SubdivisionCode: "us-or"}, wantErr: true},
		{name: "asns at limit", network: NetworkOrigin_builder{Asns: make([]uint32, 64)}},
		{name: "asns over limit", network: NetworkOrigin_builder{Asns: make([]uint32, 65)}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUsageEvent(UsageEvent_builder{Network: tt.network.Build()}.Build())
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestOAuthUsageValidation(t *testing.T) {
	tooManyScopes := make([]string, 257)
	for i := range tooManyScopes {
		tooManyScopes[i] = "scope"
	}
	tests := []struct {
		name    string
		oauth   OAuthUsage_builder
		wantErr bool
	}{
		{name: "unset fields", oauth: OAuthUsage_builder{}},
		{name: "authorize", oauth: OAuthUsage_builder{
			Action:          OAuthUsage_ACTION_AUTHORIZE,
			SourceEventName: "authorize",
			Scopes:          []string{"https://www.googleapis.com/auth/userinfo.email"},
			ClientType:      "WEB",
		}},
		{name: "empty scope", oauth: OAuthUsage_builder{Scopes: []string{""}}, wantErr: true},
		{name: "scope over max bytes", oauth: OAuthUsage_builder{Scopes: []string{strings.Repeat("a", 513)}}, wantErr: true},
		{name: "scopes at limit", oauth: OAuthUsage_builder{Scopes: tooManyScopes[:256]}},
		{name: "scopes over limit", oauth: OAuthUsage_builder{Scopes: tooManyScopes}, wantErr: true},
		{name: "source event name over max bytes", oauth: OAuthUsage_builder{SourceEventName: strings.Repeat("a", 257)}, wantErr: true},
		{name: "client type over max bytes", oauth: OAuthUsage_builder{ClientType: strings.Repeat("a", 257)}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUsageEvent(UsageEvent_builder{Oauth: tt.oauth.Build()}.Build())
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
