package field

type FieldTag string

// Field tags can be anything, but it's nice to avoid magic strings, so this file defines some common ones.

const (
	FieldTagCredential        FieldTag = "credential"
	FieldTagAPIKey            FieldTag = "api_key"
	FieldTagCertificate       FieldTag = "certificate"
	FieldTagOauthClientSecret FieldTag = "oauth_client_secret" //nolint:gosec // this is not a credential
	FieldTagPassword          FieldTag = "password"
	FieldTagSSHKey            FieldTag = "ssh_key"
)
