package field

import "github.com/conductorone/baton-sdk/pkg/logging"

var (
	createTicketField           = BoolField("create-ticket", WithHidden(true), WithDescription("Create ticket"), WithPersistent(true))
	bulkCreateTicketField       = BoolField("bulk-create-ticket", WithHidden(true), WithDescription("Bulk create tickets"), WithPersistent(true))
	bulkTicketTemplatePathField = StringField("bulk-ticket-template-path", WithHidden(true), WithDescription("A JSON file describing the ticket to create"), WithPersistent(true))
	getTicketField              = BoolField("get-ticket", WithHidden(true), WithDescription("Get ticket"), WithPersistent(true))
	ListTicketSchemasField      = BoolField("list-ticket-schemas", WithHidden(true), WithDescription("List ticket schemas"), WithPersistent(true))
	provisioningField           = BoolField("provisioning", WithShortHand("p"), WithDescription("This must be set in order for provisioning actions to be enabled"), WithPersistent(true))
	TicketingField              = BoolField("ticketing", WithDescription("This must be set to enable ticketing support"), WithPersistent(true))
	c1zTmpDirField              = StringField("c1z-temp-dir", WithHidden(true), WithDescription("The directory to store temporary files in. It must exist, "+
		"and write access is required. Defaults to the OS temporary directory."), WithPersistent(true))
	clientIDField              = StringField("client-id", WithDescription("The client ID used to authenticate with ConductorOne"), WithPersistent(true))
	clientSecretField          = StringField("client-secret", WithDescription("The client secret used to authenticate with ConductorOne"), WithPersistent(true))
	createAccountEmailField    = StringField("create-account-email", WithHidden(true), WithDescription("The email of the account to create"), WithPersistent(true))
	createAccountLoginField    = StringField("create-account-login", WithHidden(true), WithDescription("The login of the account to create"), WithPersistent(true))
	deleteResourceField        = StringField("delete-resource", WithHidden(true), WithDescription("The id of the resource to delete"), WithPersistent(true))
	deleteResourceTypeField    = StringField("delete-resource-type", WithHidden(true), WithDescription("The type of the resource to delete"), WithPersistent(true))
	eventFeedField             = StringField("event-feed", WithHidden(true), WithDescription("Read feed events to stdout"), WithPersistent(true))
	fileField                  = StringField("file", WithShortHand("f"), WithDefaultValue("sync.c1z"), WithDescription("The path to the c1z file to sync with"), WithPersistent(true))
	grantEntitlementField      = StringField("grant-entitlement", WithHidden(true), WithDescription("The id of the entitlement to grant to the supplied principal"), WithPersistent(true))
	grantPrincipalField        = StringField("grant-principal", WithHidden(true), WithDescription("The id of the resource to grant the entitlement to"), WithPersistent(true))
	grantPrincipalTypeField    = StringField("grant-principal-type", WithHidden(true), WithDescription("The resource type of the principal to grant the entitlement to"), WithPersistent(true))
	logFormatField             = StringField("log-format", WithDefaultValue(logging.LogFormatJSON), WithDescription("The output format for logs: json, console"), WithPersistent(true))
	revokeGrantField           = StringField("revoke-grant", WithHidden(true), WithDescription("The grant to revoke"), WithPersistent(true))
	rotateCredentialsField     = StringField("rotate-credentials", WithHidden(true), WithDescription("The id of the resource to rotate credentials on"), WithPersistent(true))
	rotateCredentialsTypeField = StringField("rotate-credentials-type", WithHidden(true), WithDescription("The type of the resource to rotate credentials on"), WithPersistent(true))
	ticketIDField              = StringField("ticket-id", WithHidden(true), WithDescription("The ID of the ticket to get"), WithPersistent(true))
	ticketTemplatePathField    = StringField("ticket-template-path", WithHidden(true), WithDescription("A JSON file describing the ticket to create"), WithPersistent(true))
	logLevelField              = StringField("log-level", WithDefaultValue("info"), WithDescription("The log level: debug, info, warn, error"), WithPersistent(true))
	skipFullSync               = BoolField("skip-full-sync", WithDescription("This must be set to skip a full sync"), WithPersistent(true))
)

// DefaultFields list the default fields expected in every single connector.
var DefaultFields = []SchemaField{
	createTicketField,
	bulkCreateTicketField,
	bulkTicketTemplatePathField,
	getTicketField,
	ListTicketSchemasField,
	provisioningField,
	TicketingField,
	c1zTmpDirField,
	clientIDField,
	clientSecretField,
	createAccountEmailField,
	createAccountLoginField,
	deleteResourceField,
	deleteResourceTypeField,
	eventFeedField,
	fileField,
	grantEntitlementField,
	grantPrincipalField,
	grantPrincipalTypeField,
	logFormatField,
	revokeGrantField,
	rotateCredentialsField,
	rotateCredentialsTypeField,
	ticketIDField,
	ticketTemplatePathField,
	logLevelField,
	skipFullSync,
}

func IsFieldAmongDefaultList(f SchemaField) bool {
	for _, v := range DefaultFields {
		if v.FieldName == f.FieldName {
			return true
		}
	}

	return false
}

func EnsureDefaultFieldsExists(originalFields []SchemaField) []SchemaField {
	var notfound []SchemaField

	// compare the default list of fields
	// with the incoming original list of fields
	for _, d := range DefaultFields {
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
