package secretcontracts

// Content type IDs of the bundled contracts. Connectors declare these on
// secret outputs and the typed helpers return them in Payload.ContentType.
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
