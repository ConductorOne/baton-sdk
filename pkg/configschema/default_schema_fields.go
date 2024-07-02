package configschema

import (
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// defaultFields list the default fields expected in every single connector.
var defaultFields = []ConfigField{
	StringField("log-level", WithDefaultValue("info")),
	StringField("log-format", WithDefaultValue(logging.LogFormatJSON)),
	StringField("file"),
	StringField("client-id"),
	StringField("client-secret"),
	StringField("grant-entitlement"),
	StringField("grant-principal"),
	StringField("grant-principal-type"),
	StringField("revoke-grant"),
	StringField("c1z-temp-dir"),
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
