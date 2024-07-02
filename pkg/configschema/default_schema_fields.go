package configschema

import (
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// defaultFields list the default fields expected in every single connector.
var defaultFields = []ConfigField{
	StringField("log-level", WithDefaultValue("info"), WithDescription("The log level: debug, info, warn, error")),
	StringField("log-format", WithDefaultValue(logging.LogFormatJSON), WithDescription("The output format for logs: json, console")),
	StringField("file", WithDefaultValue("sync.c1z"), WithDescription("The path to the c1z file to sync with")),
	StringField("client-id"),
	StringField("client-secret"),
	StringField("grant-entitlement", WithDescription("The id of the entitlement to grant to the supplied principal")),
	StringField("grant-principal", WithDescription("The id of the resource to grant the entitlement to")),
	StringField("grant-principal-type", WithDescription("The resource type of the principal to grant the entitlement to")),
	StringField("revoke-grant", WithDescription("The grant to revoke")),
	StringField("c1z-temp-dir", WithDescription("The directory to store temporary files in. It "+
		"must exist, and write access is required. Defaults to the OS temporary directory.")), // hidden
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
