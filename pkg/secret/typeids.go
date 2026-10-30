package secret

// Content type IDs used by the credential builders.
const (
	TypePasswordV2          = "password_v2"
	TypeAPIKeyV2            = "api_key_v2" //nolint:gosec // content type ID, not a credential.
	TypeSSHKey              = "ssh_key"
	TypeOAuthClientSecretV2 = "oauth_client_secret_v2" //nolint:gosec // content type ID, not a credential.
	TypeCertificate         = "certificate"
	TypeHOTP                = "hotp"
	TypeNote                = "note"
	TypeGeneric             = "generic"
)
