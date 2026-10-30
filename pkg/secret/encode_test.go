package secretcontracts

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCredentialBuilders(t *testing.T) {
	for _, tc := range []struct {
		name   string
		encode func() (Payload, error)
		fields map[string]any
	}{
		{TypePasswordV2, (PasswordV2{Username: "alice", Password: "p\"\\\n☃", URL: "not a URL"}).Encode, map[string]any{"username": "alice", "password": "p\"\\\n☃", "url": "not a URL"}},
		{
			TypeAPIKeyV2, (APIKeyV2{KeyValue: "", Scopes: []string{"", "read", "read"}, ExpiresAt: "not a date"}).Encode,
			map[string]any{"key_value": "", "scopes": []any{"", "read", "read"}, "expires_at": "not a date"},
		},
		{TypeSSHKey, (SSHKey{PrivateKey: "not PEM", Hosts: []string{"example.test"}}).Encode, map[string]any{"private_key": "not PEM", "hosts": []any{"example.test"}}},
		{
			TypeOAuthClientSecretV2, (OAuthClientSecretV2{ClientID: "", ClientSecret: "secret", RedirectURIs: []string{"custom:callback"}}).Encode,
			map[string]any{"client_id": "", "client_secret": "secret", "redirect_uri": []any{"custom:callback"}},
		},
		{TypeCertificate, (Certificate{CertificatePEM: "not a certificate"}).Encode, map[string]any{"certificate_pem": "not a certificate"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := tc.encode()
			require.NoError(t, err)
			require.Equal(t, tc.name, payload.ContentType)
			var actual map[string]any
			require.NoError(t, json.Unmarshal(payload.Bytes, &actual))
			require.Equal(t, tc.fields, actual)
		})
	}
}

func TestEncodeJSONAcceptsArbitraryTypesAndFields(t *testing.T) {
	payload, err := EncodeJSON("future_type", map[string]any{"unknown": true, "number": 42, "meta": map[string]any{"custom": "value"}})
	require.NoError(t, err)
	require.Equal(t, "future_type", payload.ContentType)
	require.JSONEq(t, `{"unknown":true,"number":42,"meta":{"custom":"value"}}`, string(payload.Bytes))
	_, err = EncodeJSON("anything", map[string]any{"unsupported": make(chan int)})
	require.Error(t, err)
}

func TestEmptyCredentialValuesSerialize(t *testing.T) {
	for _, encode := range []func() (Payload, error){PasswordV2{}.Encode, APIKeyV2{}.Encode, SSHKey{}.Encode, OAuthClientSecretV2{}.Encode, Certificate{}.Encode} {
		payload, err := encode()
		require.NoError(t, err)
		require.True(t, json.Valid(payload.Bytes))
	}
}

func TestTextPreservesBytesWithoutAdmission(t *testing.T) {
	for _, contentType := range []string{TypeGeneric, TypeNote, TypeHOTP, "future"} {
		for _, value := range []string{"", "not an OTP URI", "☃\n", string([]byte{0xff})} {
			payload, err := EncodeText(contentType, value)
			require.NoError(t, err)
			require.Equal(t, contentType, payload.ContentType)
			require.Equal(t, []byte(value), payload.Bytes)
		}
	}
}
