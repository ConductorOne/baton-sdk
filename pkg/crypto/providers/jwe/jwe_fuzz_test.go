package jwe

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// FuzzPublicJWKFields covers the untrusted JWK parser. It must never panic,
// every rejection must be InvalidArgument, and a successful parse must be
// lossless: the parser rejects duplicate members, so the decoded member set
// re-marshals and re-parses to itself. A parse that dropped a member or
// accepted a duplicate would break that round trip.
func FuzzPublicJWKFields(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{"kty":"AKP","alg":"` + Algorithm + `","pub":""}`),
		[]byte(`{}`),
		[]byte(`[]`),
		[]byte(`{"kty":"AKP","kty":"AKP"}`),
		[]byte(`{"kty":"AKP"} trailing`),
		[]byte(`{"nested":{"a":[1,2,{"b":null}]}}`),
		[]byte(`{"":null}`),
		{0xff, 0xfe, 0xfd},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Bound the input so a fuzz case stays cheap. The declared bound itself is
		// covered by the validation matrix, and the parser no longer enforces size.
		if len(data) == 0 || len(data) > declaredMaxLen {
			return
		}

		fields, err := publicJWKFields(data)
		if err != nil {
			require.Equal(t, codes.InvalidArgument, status.Code(err), "parser rejection must be InvalidArgument: %v", err)
			return
		}

		encoded, err := json.Marshal(fields)
		require.NoError(t, err)
		reparsed, err := publicJWKFields(encoded)
		require.NoError(t, err, "re-marshalled members must parse")
		require.Len(t, reparsed, len(fields))
		for name, value := range fields {
			require.JSONEq(t, string(value), string(reparsed[name]), "member %q changed across a round trip", name)
		}
	})
}

// FuzzRecipientPreflight feeds arbitrary bytes through the full recipient
// preflight. It must never panic, every rejection must be InvalidArgument, and
// whenever a recipient is accepted the framed output must still have the pinned
// shape, so a partially validated key cannot reach the wire.
func FuzzRecipientPreflight(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{"kty":"AKP","alg":"` + Algorithm + `","pub":""}`),
		[]byte(`{}`),
		[]byte(`{"kty":"AKP","alg":"` + Algorithm + `","priv":"x"}`),
		[]byte(`{"kty":"RSA","alg":"RSA-OAEP-256","pub":"AQAB"}`),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, jwk []byte) {
		if len(jwk) > declaredMaxLen {
			return
		}
		config := configForRawJWK("recipient-1", jwk, nil)

		key, header, err := recipient(config)
		if err != nil {
			require.Equal(t, codes.InvalidArgument, status.Code(err), "preflight rejection must be InvalidArgument: %v", err)
			return
		}
		require.NotNil(t, key)
		// Built here rather than from the provider's own serializer, so the
		// comparison stays independent of the code under test.
		expectedHeader, marshalErr := json.Marshal(struct {
			Algorithm string `json:"alg"`
			KeyID     string `json:"kid"`
		}{Algorithm, "recipient-1"})
		require.NoError(t, marshalErr)
		require.Equal(t, expectedHeader, header)
		require.LessOrEqual(t, len(header), MaxProtectedHeaderBytes)

		encrypted, err := (&Provider{}).Encrypt(context.Background(), config, v2.PlaintextData_builder{Name: "m", Bytes: []byte("payload")}.Build())
		require.NoError(t, err)
		require.Equal(t,
			[]string{"aad", "ciphertext", "encrypted_key", "iv", "protected", "tag"},
			jsonMembers(t, encrypted.GetEncryptedBytes()))

		envelope := decodeJWE(t, encrypted.GetEncryptedBytes())
		require.Empty(t, envelope.IV)
		require.Empty(t, envelope.Tag)
		require.Equal(t, header, decodeRawURL(t, envelope.Protected))
		require.Len(t, decodeRawURL(t, envelope.EncryptedKey), xwingEncapsulatedKeyBytes)
		require.Len(t, decodeRawURL(t, envelope.Ciphertext), len("payload")+chacha20Poly1305TagBytes)
	})
}
