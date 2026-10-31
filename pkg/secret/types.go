package secret

// When updating these builders, check the linked Multipass definitions and codec
// and preserve JSON field names and list shapes.
// Codec: https://github.com/ductone/multipass/blob/95d5f9b7cfa923fd4d77953d051d216df377121b/crates/latchkey-client-sdk/src/secret_types/codec.rs

// PasswordV2 follows the Multipass password_v2 fields:
// https://github.com/ductone/multipass/blob/95d5f9b7cfa923fd4d77953d051d216df377121b/crates/latchkey-client-sdk/src/secret_types/definitions.rs#L239-L270
type PasswordV2 struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password"`
	URL      string `json:"url,omitempty"`
	OTPURL   string `json:"otp_url,omitempty"`
}

func (v PasswordV2) Encode() (Payload, error) {
	return encodeJSON(TypePasswordV2, v)
}

// APIKeyV2 follows the Multipass api_key_v2 fields:
// https://github.com/ductone/multipass/blob/95d5f9b7cfa923fd4d77953d051d216df377121b/crates/latchkey-client-sdk/src/secret_types/definitions.rs#L298-L314
type APIKeyV2 struct {
	KeyValue   string   `json:"key_value"`
	Provider   string   `json:"provider,omitempty"`
	BaseURL    string   `json:"base_url,omitempty"`
	Scopes     []string `json:"scopes,omitempty"`
	KeyID      string   `json:"key_id,omitempty"`
	HeaderName string   `json:"header_name,omitempty"`
	ExpiresAt  string   `json:"expires_at,omitempty"`
}

func (v APIKeyV2) Encode() (Payload, error) {
	return encodeJSON(TypeAPIKeyV2, v)
}

// SSHKey follows the Multipass ssh_key fields:
// https://github.com/ductone/multipass/blob/95d5f9b7cfa923fd4d77953d051d216df377121b/crates/latchkey-client-sdk/src/secret_types/definitions.rs#L316-L331
type SSHKey struct {
	PrivateKey     string   `json:"private_key"`
	Username       string   `json:"username,omitempty"`
	Passphrase     string   `json:"passphrase,omitempty"`
	PublicKey      string   `json:"public_key,omitempty"`
	Hosts          []string `json:"hosts,omitempty"`
	SSHCertificate string   `json:"ssh_certificate,omitempty"`
}

func (v SSHKey) Encode() (Payload, error) {
	return encodeJSON(TypeSSHKey, v)
}

// OAuthClientSecretV2 follows the Multipass oauth_client_secret_v2 fields:
// https://github.com/ductone/multipass/blob/95d5f9b7cfa923fd4d77953d051d216df377121b/crates/latchkey-client-sdk/src/secret_types/definitions.rs#L333-L352
type OAuthClientSecretV2 struct {
	ClientID         string   `json:"client_id"`
	ClientSecret     string   `json:"client_secret"`
	TokenEndpointURL string   `json:"token_endpoint_url,omitempty"`
	Scopes           []string `json:"scopes,omitempty"`
	IssuerURL        string   `json:"issuer_url,omitempty"`
	RedirectURIs     []string `json:"redirect_uri,omitempty"`
	AuthEndpointURL  string   `json:"auth_endpoint_url,omitempty"`
	Audience         string   `json:"audience,omitempty"`
	GrantType        string   `json:"grant_type,omitempty"`
}

func (v OAuthClientSecretV2) Encode() (Payload, error) {
	return encodeJSON(TypeOAuthClientSecretV2, v)
}

// Certificate follows the Multipass certificate fields:
// https://github.com/ductone/multipass/blob/95d5f9b7cfa923fd4d77953d051d216df377121b/crates/latchkey-client-sdk/src/secret_types/definitions.rs#L354-L367
type Certificate struct {
	CertificatePEM string `json:"certificate_pem"`
	PrivateKey     string `json:"private_key,omitempty"`
	ChainPEM       string `json:"chain_pem,omitempty"`
	Passphrase     string `json:"passphrase,omitempty"`
}

func (v Certificate) Encode() (Payload, error) {
	return encodeJSON(TypeCertificate, v)
}
