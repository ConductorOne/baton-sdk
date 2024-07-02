package configschema

import (
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// defaultFields list the default fields expected in every single connector.
var defaultFields = []ConfigField{
	BoolField("create-ticket", WithHidden(true), WithDescription("Create ticket")),
	BoolField("get-ticket", WithHidden(true), WithDescription("Get ticket")),
	BoolField("list-ticket-schemas", WithHidden(true), WithDescription("List ticket schemas")),
	BoolField("provisioning", WithShortHand("p"), WithDescription("This must be set in order for provisioning actions to be enabled")),
	BoolField("ticketing", WithDescription("This must be set to enable ticketing support")),
	StringField("c1z-temp-dir", WithHidden(true), WithDescription("The directory to store temporary files in. It must exist, and write access is required. Defaults to the OS temporary directory.")),
	StringField("client-id", WithDescription("The client ID used to authenticate with ConductorOne")),
	StringField("client-secret", WithDescription("The client secret used to authenticate with ConductorOne")),
	StringField("create-account-email", WithHidden(true), WithDescription("The email of the account to create")),
	StringField("create-account-login", WithHidden(true), WithDescription("The login of the account to create")),
	StringField("delete-resource", WithHidden(true), WithDescription("The id of the resource to delete")),
	StringField("delete-resource-type", WithHidden(true), WithDescription("The type of the resource to delete")),
	StringField("event-feed", WithHidden(true), WithDescription("Read feed events to stdout")),
	StringField("file", WithShortHand("f"), WithDefaultValue("sync.c1z"), WithDescription("The path to the c1z file to sync with")),
	StringField("grant-entitlement", WithHidden(true), WithDescription("The id of the entitlement to grant to the supplied principal")),
	StringField("grant-principal", WithHidden(true), WithDescription("The id of the resource to grant the entitlement to")),
	StringField("grant-principal-type", WithHidden(true), WithDescription("The resource type of the principal to grant the entitlement to")),
	StringField("log-format", WithDefaultValue(logging.LogFormatJSON), WithDescription("The output format for logs: json, console")),
	StringField("revoke-grant", WithHidden(true), WithDescription("The grant to revoke")),
	StringField("rotate-credentials", WithHidden(true), WithDescription("The id of the resource to rotate credentials on")),
	StringField("rotate-credentials-type", WithHidden(true), WithDescription("The type of the resource to rotate credentials on")),
	StringField("ticket-id", WithHidden(true), WithDescription("The ID of the ticket to get")),
	StringField("ticket-template-path", WithHidden(true), WithDescription("A JSON file describing the ticket to create")),
	StringField("log-level", WithDefaultValue("info"), WithDescription("The log level: debug, info, warn, error")),
}

func ensureDefaultFieldsExists(originalFields []ConfigField) []ConfigField {
	var notfound []ConfigField

	// compare the default list of fields
	// with the incoming original list of fields
	for _, d := range defaultFields {
		found := false
		for _, o := range originalFields {
			if d.FieldName == o.FieldName {
				found = true
			}
		}

		if !found {
			notfound = append(notfound, d)
		}
	}

	notfound = append(notfound, originalFields...)

	return notfound
}
