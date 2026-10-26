package secretcontracts

// PasswordV2 is the password_v2 document.
type PasswordV2 struct {
	Username string
	Password string
	URL      string
	OTPURL   string
}

func (v PasswordV2) Encode() (Payload, error) {
	return EncodeJSON(TypePasswordV2, map[string]any{
		"username": v.Username, "password": v.Password, "url": v.URL, "otp_url": v.OTPURL,
	})
}

// APIKeyV2 is the api_key_v2 document.
type APIKeyV2 struct {
	KeyValue   string
	Provider   string
	BaseURL    string
	Scopes     []string
	KeyID      string
	HeaderName string
	ExpiresAt  string
}

func (v APIKeyV2) Encode() (Payload, error) {
	return EncodeJSON(TypeAPIKeyV2, map[string]any{
		"key_value": v.KeyValue, "provider": v.Provider, "base_url": v.BaseURL,
		"scopes": nonNilStrings(v.Scopes), "key_id": v.KeyID, "header_name": v.HeaderName, "expires_at": v.ExpiresAt,
	})
}

// SSHKey is the ssh_key document.
type SSHKey struct {
	PrivateKey     string
	Username       string
	Passphrase     string
	PublicKey      string
	Hosts          []string
	SSHCertificate string
}

func (v SSHKey) Encode() (Payload, error) {
	return EncodeJSON(TypeSSHKey, map[string]any{
		"private_key": v.PrivateKey, "username": v.Username, "passphrase": v.Passphrase,
		"public_key": v.PublicKey, "hosts": nonNilStrings(v.Hosts), "ssh_certificate": v.SSHCertificate,
	})
}

// OAuthClientSecretV2 is the oauth_client_secret_v2 document.
type OAuthClientSecretV2 struct {
	ClientID         string
	ClientSecret     string
	TokenEndpointURL string
	Scopes           []string
	IssuerURL        string
	RedirectURIs     []string
	AuthEndpointURL  string
	Audience         string
	GrantType        string
}

func (v OAuthClientSecretV2) Encode() (Payload, error) {
	return EncodeJSON(TypeOAuthClientSecretV2, map[string]any{
		"client_id": v.ClientID, "client_secret": v.ClientSecret, "token_endpoint_url": v.TokenEndpointURL,
		"scopes": nonNilStrings(v.Scopes), "issuer_url": v.IssuerURL, "redirect_uri": nonNilStrings(v.RedirectURIs),
		"auth_endpoint_url": v.AuthEndpointURL, "audience": v.Audience, "grant_type": v.GrantType,
	})
}

// Certificate is the certificate document.
type Certificate struct {
	CertificatePEM string
	PrivateKey     string
	ChainPEM       string
	Passphrase     string
}

func (v Certificate) Encode() (Payload, error) {
	return EncodeJSON(TypeCertificate, map[string]any{
		"certificate_pem": v.CertificatePEM, "private_key": v.PrivateKey, "chain_pem": v.ChainPEM, "passphrase": v.Passphrase,
	})
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
