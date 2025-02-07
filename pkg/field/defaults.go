package field

import "github.com/conductorone/baton-sdk/pkg/logging"

var (
	createTicketField           = BoolField("create-ticket", WithHidden(true), WithDescription("Create ticket"), WithPersistent(true), WithOnlyCLI())
	bulkCreateTicketField       = BoolField("bulk-create-ticket", WithHidden(true), WithDescription("Bulk create tickets"), WithPersistent(true), WithOnlyCLI())
	bulkTicketTemplatePathField = StringField("bulk-ticket-template-path", WithHidden(true), WithDescription("A JSON file describing the ticket to create"), WithPersistent(true), WithOnlyCLI())
	getTicketField              = BoolField("get-ticket", WithHidden(true), WithDescription("Get ticket"), WithPersistent(true), WithOnlyCLI())
	ListTicketSchemasField      = BoolField("list-ticket-schemas", WithHidden(true), WithDescription("List ticket schemas"), WithPersistent(true), WithOnlyCLI())
	provisioningField           = BoolField("provisioning", WithShortHand("p"), WithDescription("This must be set in order for provisioning actions to be enabled"),
		WithPersistent(true), WithOnlyCLI())
	TicketingField = BoolField("ticketing", WithDescription("This must be set to enable ticketing support"), WithPersistent(true), WithOnlyCLI())
	c1zTmpDirField = StringField("c1z-temp-dir", WithHidden(true), WithDescription("The directory to store temporary files in. It must exist, "+
		"and write access is required. Defaults to the OS temporary directory."), WithPersistent(true), WithOnlyCLI())
	clientIDField             = StringField("client-id", WithDescription("The client ID used to authenticate with ConductorOne"), WithPersistent(true), WithOnlyCLI(), WithOnlyCLI())
	clientSecretField         = StringField("client-secret", WithDescription("The client secret used to authenticate with ConductorOne"), WithPersistent(true), WithOnlyCLI(), WithOnlyCLI())
	createAccountEmailField   = StringField("create-account-email", WithHidden(true), WithDescription("The email of the account to create"), WithPersistent(true), WithOnlyCLI())
	createAccountLoginField   = StringField("create-account-login", WithHidden(true), WithDescription("The login of the account to create"), WithPersistent(true), WithOnlyCLI())
	createAccountProfileField = StringField("create-account-profile",
		WithHidden(true),
		WithDescription("JSON-formatted object of map keys and values like '{ 'key': 'value' }'"),
		WithPersistent(true), WithOnlyCLI())
	deleteResourceField     = StringField("delete-resource", WithHidden(true), WithDescription("The id of the resource to delete"), WithPersistent(true), WithOnlyCLI())
	deleteResourceTypeField = StringField("delete-resource-type", WithHidden(true), WithDescription("The type of the resource to delete"), WithPersistent(true), WithOnlyCLI())
	eventFeedField          = StringField("event-feed", WithHidden(true), WithDescription("Read feed events to stdout"), WithPersistent(true), WithOnlyCLI())
	fileField               = StringField("file", WithShortHand("f"), WithDefaultValue("sync.c1z"), WithDescription("The path to the c1z file to sync with"), WithPersistent(true), WithOnlyCLI())
	grantEntitlementField   = StringField("grant-entitlement", WithHidden(true), WithDescription("The id of the entitlement to grant to the supplied principal"),
		WithPersistent(true), WithOnlyCLI())
	grantPrincipalField = StringField("grant-principal", WithHidden(true), WithDescription("The id of the resource to grant the entitlement to"),
		WithPersistent(true), WithOnlyCLI())
	grantPrincipalTypeField = StringField("grant-principal-type", WithHidden(true), WithDescription("The resource type of the principal to grant the entitlement to"),
		WithPersistent(true), WithOnlyCLI())
	logFormatField             = StringField("log-format", WithDefaultValue(logging.LogFormatJSON), WithDescription("The output format for logs: json, console"), WithPersistent(true), WithOnlyCLI())
	revokeGrantField           = StringField("revoke-grant", WithHidden(true), WithDescription("The grant to revoke"), WithPersistent(true), WithOnlyCLI())
	rotateCredentialsField     = StringField("rotate-credentials", WithHidden(true), WithDescription("The id of the resource to rotate credentials on"), WithPersistent(true), WithOnlyCLI())
	rotateCredentialsTypeField = StringField("rotate-credentials-type", WithHidden(true), WithDescription("The type of the resource to rotate credentials on"), WithPersistent(true), WithOnlyCLI())
	ticketIDField              = StringField("ticket-id", WithHidden(true), WithDescription("The ID of the ticket to get"), WithPersistent(true), WithOnlyCLI())
	ticketTemplatePathField    = StringField("ticket-template-path", WithHidden(true), WithDescription("A JSON file describing the ticket to create"), WithPersistent(true), WithOnlyCLI())
	logLevelField              = StringField("log-level", WithDefaultValue("info"), WithDescription("The log level: debug, info, warn, error"), WithPersistent(true), WithIsOps())
	skipFullSync               = BoolField("skip-full-sync", WithDescription("This must be set to skip a full sync"), WithPersistent(true), WithOnlyCLI())
	otelCollectorEndpoint      = StringField("otel-collector-endpoint", WithDescription("The endpoint of the OpenTelemetry collector to send observability data to"),
		WithPersistent(true), WithOnlyCLI())
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
	createAccountProfileField,
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
	otelCollectorEndpoint,
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
