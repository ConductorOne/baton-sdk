package secretcontracts

// PasswordV2 is the password_v2 document.
type PasswordV2 struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password"`
	URL      string `json:"url,omitempty"`
	OTPURL   string `json:"otp_url,omitempty"`
}

func (v PasswordV2) Encode() (Payload, error) {
	return encodeJSON(TypePasswordV2, v)
}

// APIKeyV2 is the api_key_v2 document.
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

// SSHKey is the ssh_key document.
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

// OAuthClientSecretV2 is the oauth_client_secret_v2 document.
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

// Certificate is the certificate document.
type Certificate struct {
	CertificatePEM string `json:"certificate_pem"`
	PrivateKey     string `json:"private_key,omitempty"`
	ChainPEM       string `json:"chain_pem,omitempty"`
	Passphrase     string `json:"passphrase,omitempty"`
}

func (v Certificate) Encode() (Payload, error) {
	return encodeJSON(TypeCertificate, v)
}
