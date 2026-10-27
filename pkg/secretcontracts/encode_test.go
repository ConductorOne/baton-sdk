package secretcontracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func fixtureText(t *testing.T, relative string) string {
	t.Helper()
	return string(readFile(t, filepath.Join("bundle", "fixtures", relative)))
}

func TestEncodeJSONOmitsBlankOptionalsAndKeepsDeclaredOrder(t *testing.T) {
	payload, err := PasswordV2{Username: " ", Password: "hunter2", URL: "https://example.com", OTPURL: ""}.Encode()
	require.NoError(t, err)
	require.Equal(t, "password_v2", payload.ContentType)
	require.Equal(t, `{"password":"hunter2","url":"https://example.com"}`, string(payload.Bytes))
	require.NoError(t, ValidateProducer(payload.ContentType, payload.Bytes))
}

func TestEncodeJSONDoesNotEscapeHTMLCharacters(t *testing.T) {
	payload, err := PasswordV2{Password: "a<b>&c"}.Encode()
	require.NoError(t, err)
	require.Equal(t, `{"password":"a<b>&c"}`, string(payload.Bytes))
}

func TestTypedHelpersProduceValidPayloads(t *testing.T) {
	key := fixtureText(t, "valid/ssh_key/full.json")
	require.NotEmpty(t, key)
	certificate := mustParse(t, readFile(t, filepath.Join("bundle", "fixtures", "valid", "certificate", "full.json")))["certificate_pem"].(string)
	sshKey := mustParse(t, readFile(t, filepath.Join("bundle", "fixtures", "valid", "ssh_key", "full.json")))
	for name, encode := range map[string]func() (Payload, error){
		"api_key_v2": APIKeyV2{KeyValue: "k", Scopes: []string{"a", "b"}, ExpiresAt: "2027-02-28", HeaderName: "X-Key", BaseURL: "https://api.example.com"}.Encode,
		"ssh_key":    SSHKey{PrivateKey: sshKey["private_key"].(string), Username: "deploy", Hosts: []string{"h.example.com"}}.Encode,
		//nolint:gosec // placeholder value, not a credential.
		"oauth_client_secret_v2": OAuthClientSecretV2{
			ClientID: "c", ClientSecret: "placeholder", RedirectURIs: []string{"com.example.app:/cb"}, TokenEndpointURL: "https://idp.example.com/token",
		}.Encode,
		"certificate": Certificate{CertificatePEM: certificate}.Encode,
	} {
		payload, err := encode()
		require.NoError(t, err, name)
		require.Equal(t, name, payload.ContentType)
		kind, err := ValidateStored(payload.ContentType, payload.Bytes)
		require.NoError(t, err, name)
		require.Equal(t, StoredTyped, kind)
	}
}

func TestEncodeJSONRefusesBadInput(t *testing.T) {
	_, err := EncodeJSON("password_v2", map[string]any{"password": "p", "nope": "x"})
	require.ErrorContains(t, err, "unknown_field")
	_, err = EncodeJSON("password_v2", map[string]any{"password": 7})
	require.ErrorContains(t, err, "wrong_type")
	_, err = EncodeJSON("password_v2", map[string]any{"username": "u"})
	require.Error(t, err, "required field missing")
	_, err = EncodeJSON("note", map[string]any{"note": "x"})
	require.ErrorContains(t, err, "not_a_json_type")
	_, err = EncodeJSON("unheard_of", map[string]any{})
	require.ErrorContains(t, err, "unknown_content_type")
	_, err = PasswordV2{Password: "p", URL: "ftp://example.com"}.Encode()
	require.ErrorContains(t, err, "http_url")
}

func TestEncodeTextIsVerbatim(t *testing.T) {
	payload, err := EncodeText("note", "  keep \n")
	require.NoError(t, err)
	require.Equal(t, "  keep \n", string(payload.Bytes))
	payload, err = EncodeText("generic", "{\"looks\":\"like json\"}")
	require.NoError(t, err)
	require.Equal(t, "generic", payload.ContentType)
	_, err = EncodeText("hotp", "otpauth://totp/x?secret=JBSWY3DPEHPK3PXP")
	require.Error(t, err)
	_, err = EncodeText("password_v2", "x")
	require.ErrorContains(t, err, "not_a_text_type")
	_, err = EncodeText("note", "   ")
	require.Error(t, err)
}

func TestStoredAcceptsLegacyAndUnknownValues(t *testing.T) {
	kind, err := ValidateStored("generic", []byte{0xff, 0x00, 0x61})
	require.NoError(t, err)
	require.Equal(t, StoredOpaque, kind)
	kind, err = ValidateStored("totp", []byte("anything"))
	require.NoError(t, err)
	require.Equal(t, StoredOpaque, kind)
	_, err = ValidateStored("note", []byte{0xff})
	require.Error(t, err, "a typed text value must be UTF-8")
	require.Error(t, ValidateProducer("generic", []byte{0xff}), "a producer must not write non-UTF-8")
	require.Error(t, ValidateProducer("totp", []byte("x")), "a producer must not write an undefined type")
}

func TestDuplicateKeysAreRejectedAtAnyDepthAndAfterUnescaping(t *testing.T) {
	for _, raw := range []string{
		`{"password":"a","password":"b"}`,
		`{"password":"a","password":"b"}`,
		`{"password":"a","scopes":[{"k":1,"k":2}]}`,
	} {
		_, perr := parseDocument([]byte(raw))
		require.NotNil(t, perr, raw)
		require.Equal(t, "duplicate_key", perr.Rule, raw)
	}
	_, perr := parseDocument([]byte(`{"a":"🔑"}`))
	require.Nil(t, perr, "a valid surrogate pair is accepted")
	for _, raw := range []string{`{"a":"\ud83d"}`, `{"a":"\udd11"}`, `{"a":"\ud83dx"}`} {
		_, perr := parseDocument([]byte(raw))
		require.NotNil(t, perr, raw)
	}
	_, perr = parseDocument([]byte(`{"a":"b"} {}`))
	require.NotNil(t, perr)
	_, err := os.Stat("bundle/contracts.json")
	require.NoError(t, err)
}

func TestEncodersRefuseInvalidUTF8InsteadOfRewritingIt(t *testing.T) {
	bad := string([]byte{0xff})
	_, err := APIKeyV2{KeyValue: bad}.Encode()
	require.ErrorContains(t, err, "invalid_utf8")
	_, err = APIKeyV2{KeyValue: "k", Scopes: []string{"ok", "a" + bad}}.Encode()
	require.ErrorContains(t, err, "invalid_utf8")
	_, err = EncodeText("note", "a"+bad)
	require.ErrorContains(t, err, "invalid_utf8")
	_, err = EncodeText("generic", bad)
	require.ErrorContains(t, err, "invalid_utf8")
}

func TestEncodersKeepValidUnicodeBytesExactly(t *testing.T) {
	value := "pä ss\u00a0\U0001f511\ufffd\"\\"
	payload, err := APIKeyV2{KeyValue: value, Scopes: []string{"é", "\U0001f511"}}.Encode()
	require.NoError(t, err)
	document := mustParse(t, payload.Bytes)
	require.Equal(t, value, document["key_value"])
	require.Equal(t, []any{"é", "\U0001f511"}, document["scopes"])
	text, err := EncodeText("note", value)
	require.NoError(t, err)
	require.Equal(t, value, string(text.Bytes))
}

func TestErrorsNeverReflectUndeclaredNames(t *testing.T) {
	name := "abcdef0123456789abcdef0123456789"
	_, err := EncodeJSON("password_v2", map[string]any{"password": "p", name: "x"})
	assertNoName(t, err, name)
	err = ValidateProducer("password_v2", []byte(`{"password":"p","`+name+`":"a","`+name+`":"b"}`))
	assertNoName(t, err, name)
	err = ValidateProducer("password_v2", []byte(`{"password":"p","`+name+`":"a"}`))
	assertNoName(t, err, name)
	_, err = ValidateStored("password_v2", []byte(`{"password":"p","`+name+`":"a"}`))
	assertNoName(t, err, name)
	err = ValidateProducer("password_v2", []byte(`{"password":"p","password":"q"}`))
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "password", failure.Field, "a declared name is kept")
}

func assertNoName(t *testing.T, err error, name string) {
	t.Helper()
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.Empty(t, failure.Field)
	require.NotContains(t, failure.Error(), name)
	require.NotContains(t, err.Error(), name)
}

func TestOversizedInputIsRejectedBySizeGate(t *testing.T) {
	big := []byte(`{"password":"` + strings.Repeat("a", 524288) + `"}`)
	for _, producer := range []bool{true, false} {
		var err error
		if producer {
			err = ValidateProducer("password_v2", big)
		} else {
			_, err = ValidateStored("password_v2", big)
		}
		var failure *Error
		require.ErrorAs(t, err, &failure)
		require.Equal(t, LayerLimit, failure.Layer)
		require.Equal(t, "document_too_large", failure.Rule)
	}
	huge := make([]byte, 1024*1024+1)
	_, err := ValidateStored("generic", huge)
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "value_too_large", failure.Rule)
	require.Error(t, ValidateProducer("generic", huge))
}
