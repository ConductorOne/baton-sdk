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

func TestUsageEventNetworkValidation(t *testing.T) {
	tests := []struct {
		name    string
		network UsageEvent_Network_builder
		wantErr bool
	}{
		{name: "unset fields", network: UsageEvent_Network_builder{}},
		{name: "ipv4", network: UsageEvent_Network_builder{IpAddress: "203.0.113.7"}},
		{name: "ipv6", network: UsageEvent_Network_builder{IpAddress: "2001:db8::1"}},
		{name: "ip with port", network: UsageEvent_Network_builder{IpAddress: "203.0.113.7:443"}, wantErr: true},
		{name: "ip list", network: UsageEvent_Network_builder{IpAddress: "203.0.113.7, 198.51.100.1"}, wantErr: true},
		{name: "ip placeholder", network: UsageEvent_Network_builder{IpAddress: "unknown"}, wantErr: true},
		{name: "country", network: UsageEvent_Network_builder{CountryCode: "US"}},
		{name: "country lowercase", network: UsageEvent_Network_builder{CountryCode: "us"}, wantErr: true},
		{name: "country alpha-3", network: UsageEvent_Network_builder{CountryCode: "USA"}, wantErr: true},
		{name: "country digit", network: UsageEvent_Network_builder{CountryCode: "1A"}, wantErr: true},
		{name: "subdivision", network: UsageEvent_Network_builder{SubdivisionCode: "US-OR"}},
		{name: "subdivision three letters", network: UsageEvent_Network_builder{SubdivisionCode: "GB-ENG"}},
		{name: "subdivision alphanumeric", network: UsageEvent_Network_builder{SubdivisionCode: "FR-75C"}},
		{name: "subdivision without country", network: UsageEvent_Network_builder{SubdivisionCode: "OR"}, wantErr: true},
		{name: "subdivision lowercase", network: UsageEvent_Network_builder{SubdivisionCode: "us-or"}, wantErr: true},
		{name: "asns at limit", network: UsageEvent_Network_builder{Asns: make([]uint32, 64)}},
		{name: "asns over limit", network: UsageEvent_Network_builder{Asns: make([]uint32, 65)}, wantErr: true},
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

func TestUsageEventOAuthValidation(t *testing.T) {
	tooManyScopes := make([]string, 257)
	for i := range tooManyScopes {
		tooManyScopes[i] = "scope"
	}
	tests := []struct {
		name    string
		oauth   UsageEvent_OAuth_builder
		wantErr bool
	}{
		{name: "unset fields", oauth: UsageEvent_OAuth_builder{}},
		{name: "authorize", oauth: UsageEvent_OAuth_builder{
			Action:          UsageEvent_OAuth_ACTION_AUTHORIZE,
			SourceEventName: "authorize",
			Scopes:          []string{"https://www.googleapis.com/auth/userinfo.email"},
			ClientType:      "WEB",
		}},
		{name: "empty scope", oauth: UsageEvent_OAuth_builder{Scopes: []string{""}}, wantErr: true},
		{name: "scope over max bytes", oauth: UsageEvent_OAuth_builder{Scopes: []string{strings.Repeat("a", 513)}}, wantErr: true},
		{name: "scopes at limit", oauth: UsageEvent_OAuth_builder{Scopes: tooManyScopes[:256]}},
		{name: "scopes over limit", oauth: UsageEvent_OAuth_builder{Scopes: tooManyScopes}, wantErr: true},
		{name: "source event name over max bytes", oauth: UsageEvent_OAuth_builder{SourceEventName: strings.Repeat("a", 257)}, wantErr: true},
		{name: "client type over max bytes", oauth: UsageEvent_OAuth_builder{ClientType: strings.Repeat("a", 257)}, wantErr: true},
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
