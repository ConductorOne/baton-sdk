package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	// KeyIDField is the App Store Connect API Key ID, shown next to the key in Users and Access.
	KeyIDField = field.StringField(
		"key-id",
		field.WithDisplayName("Key ID"),
		field.WithDescription("The App Store Connect API key ID, found under Users and Access > Integrations > App Store Connect API. ($BATON_KEY_ID)"),
		field.WithPlaceholder("2X9R4HXF34"),
		field.WithRequired(true),
	)

	// IssuerIDField identifies the team the API key belongs to.
	IssuerIDField = field.StringField(
		"issuer-id",
		field.WithDisplayName("Issuer ID"),
		field.WithDescription("The App Store Connect API issuer ID for your team. ($BATON_ISSUER_ID)"),
		field.WithPlaceholder("57246542-96fe-1a63-e053-0824d011072a"),
		field.WithRequired(true),
	)

	// PrivateKeyField carries the contents of the .p8 file Apple lets you download exactly once.
	PrivateKeyField = field.FileUploadField(
		"private-key",
		[]string{".p8"},
		field.WithDisplayName("Private Key (.p8)"),
		field.WithDescription("The contents of the .p8 private key file downloaded from App Store Connect. ($BATON_PRIVATE_KEY)"),
		field.WithIsSecret(true),
	)

	// PrivateKeyPathField is the local-CLI alternative to pasting the key material.
	PrivateKeyPathField = field.StringField(
		"private-key-path",
		field.WithDisplayName("Private Key Path"),
		field.WithDescription("Path to the .p8 private key file downloaded from App Store Connect. ($BATON_PRIVATE_KEY_PATH)"),
		field.WithExportTarget(field.ExportTargetCLIOnly),
	)

	// BaseURLField exists so tests and diagnostics can point the connector at a stand-in API.
	BaseURLField = field.StringField(
		"base-url",
		field.WithDescription("Override the App Store Connect API URL (for testing)"),
		field.WithHidden(true),
		field.WithExportTarget(field.ExportTargetCLIOnly),
	)
)

//go:generate go run ./gen
var Config = field.NewConfiguration(
	[]field.SchemaField{
		KeyIDField,
		IssuerIDField,
		PrivateKeyField,
		PrivateKeyPathField,
		BaseURLField,
	},
	field.WithConnectorDisplayName("Apple App Store Connect"),
	field.WithHelpUrl("/docs/baton/app-store-connect"),
	field.WithIconUrl("/static/app-icons/appstoreconnect.svg"),
	field.WithConstraints(
		field.FieldsMutuallyExclusive(PrivateKeyField, PrivateKeyPathField),
		field.FieldsAtLeastOneUsed(PrivateKeyField, PrivateKeyPathField),
	),
)
