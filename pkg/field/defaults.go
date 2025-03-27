package field

import "github.com/conductorone/baton-sdk/pkg/logging"

const (
	OtelCollectorEndpointFieldName            = "otel-collector-endpoint"
	OtelCollectorEndpointTLSCertPathFieldName = "otel-collector-endpoint-tls-cert-path"
	OtelCollectorEndpointTLSCertFieldName     = "otel-collector-endpoint-tls-cert"
	OtelCollectorEndpointTLSInsecureFieldName = "otel-collector-endpoint-tls-insecure"
	OtelTracingDisabledFieldName              = "otel-tracing-disabled"
	OtelLoggingDisabledFieldName              = "otel-logging-disabled"
)

var (
	createTicketField           = BoolField("create-ticket", WithHidden(true), WithDescription("Create ticket"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	bulkCreateTicketField       = BoolField("bulk-create-ticket", WithHidden(true), WithDescription("Bulk create tickets"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	bulkTicketTemplatePathField = StringField("bulk-ticket-template-path", WithHidden(true), WithDescription("A JSON file describing the ticket to create"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	getTicketField         = BoolField("get-ticket", WithHidden(true), WithDescription("Get ticket"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	ListTicketSchemasField = BoolField("list-ticket-schemas", WithHidden(true), WithDescription("List ticket schemas"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	provisioningField      = BoolField("provisioning", WithShortHand("p"), WithDescription("This must be set in order for provisioning actions to be enabled"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	TicketingField = BoolField("ticketing", WithDescription("This must be set to enable ticketing support"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	c1zTmpDirField = StringField("c1z-temp-dir", WithHidden(true), WithDescription("The directory to store temporary files in. It must exist, "+
		"and write access is required. Defaults to the OS temporary directory."), WithPersistent(true), WithExportTarget(ExportTargetNone))
	clientIDField             = StringField("client-id", WithDescription("The client ID used to authenticate with ConductorOne"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	clientSecretField         = StringField("client-secret", WithDescription("The client secret used to authenticate with ConductorOne"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	createAccountEmailField   = StringField("create-account-email", WithHidden(true), WithDescription("The email of the account to create"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	createAccountLoginField   = StringField("create-account-login", WithHidden(true), WithDescription("The login of the account to create"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	createAccountProfileField = StringField("create-account-profile",
		WithHidden(true),
		WithDescription("JSON-formatted object of map keys and values like '{ 'key': 'value' }'"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	deleteResourceField     = StringField("delete-resource", WithHidden(true), WithDescription("The id of the resource to delete"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	deleteResourceTypeField = StringField("delete-resource-type", WithHidden(true), WithDescription("The type of the resource to delete"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	eventFeedField          = StringField("event-feed", WithHidden(true), WithDescription("Read feed events to stdout"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	fileField               = StringField("file", WithShortHand("f"), WithDefaultValue("sync.c1z"), WithDescription("The path to the c1z file to sync with"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	grantEntitlementField = StringField("grant-entitlement", WithHidden(true), WithDescription("The id of the entitlement to grant to the supplied principal"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	grantPrincipalField = StringField("grant-principal", WithHidden(true), WithDescription("The id of the resource to grant the entitlement to"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	grantPrincipalTypeField = StringField("grant-principal-type", WithHidden(true), WithDescription("The resource type of the principal to grant the entitlement to"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	logFormatField = StringField("log-format", WithDefaultValue(logging.LogFormatJSON), WithDescription("The output format for logs: json, console"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	revokeGrantField       = StringField("revoke-grant", WithHidden(true), WithDescription("The grant to revoke"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	rotateCredentialsField = StringField("rotate-credentials", WithHidden(true), WithDescription("The id of the resource to rotate credentials on"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	rotateCredentialsTypeField = StringField("rotate-credentials-type", WithHidden(true), WithDescription("The type of the resource to rotate credentials on"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	ticketIDField           = StringField("ticket-id", WithHidden(true), WithDescription("The ID of the ticket to get"), WithPersistent(true), WithExportTarget(ExportTargetNone))
	ticketTemplatePathField = StringField("ticket-template-path", WithHidden(true), WithDescription("A JSON file describing the ticket to create"),
		WithPersistent(true), WithExportTarget(ExportTargetNone))
	logLevelField = StringField("log-level", WithDefaultValue("info"), WithDescription("The log level: debug, info, warn, error"), WithPersistent(true),
		WithExportTarget(ExportTargetOps))
	skipFullSync = BoolField("skip-full-sync", WithDescription("This must be set to skip a full sync"), WithPersistent(true), WithExportTarget(ExportTargetNone))

	otelCollectorEndpoint = StringField(OtelCollectorEndpointFieldName,
		WithDescription("The endpoint of the OpenTelemetry collector to send observability data to (used for both tracing and logging if specific endpoints are not provided)"),
		WithPersistent(true), WithExportTarget(ExportTargetOps))
	otelCollectorEndpointTLSCertPath = StringField(OtelCollectorEndpointTLSCertPathFieldName,
		WithDescription("Path to a file containing a PEM-encoded certificate to use as a CA for TLS connections to the OpenTelemetry collector"),
		WithPersistent(true), WithHidden(true), WithExportTarget(ExportTargetOps))
	otelCollectorEndpointTlSCert = StringField(OtelCollectorEndpointTLSCertFieldName,
		WithDescription("A PEM-encoded certificate to use as a CA for TLS connections to the OpenTelemetry collector"),
		WithPersistent(true), WithHidden(true), WithExportTarget(ExportTargetOps))
	otelCollectorEndpointTlSInsecure = BoolField(OtelCollectorEndpointTLSInsecureFieldName,
		WithDescription("Allow insecure connections to the OpenTelemetry collector"),
		WithPersistent(true), WithHidden(true), WithExportTarget(ExportTargetOps))
	otelTracingDisabled = BoolField(OtelTracingDisabledFieldName,
		WithDescription("Disable OpenTelemetry tracing"), WithDefaultValue(false),
		WithPersistent(true), WithHidden(true), WithExportTarget(ExportTargetOps))
	otelLoggingDisabled = BoolField(OtelLoggingDisabledFieldName,
		WithDescription("Disable OpenTelemetry logging"), WithDefaultValue(false),
		WithPersistent(true), WithHidden(true), WithExportTarget(ExportTargetOps))

	externalResourceC1ZField = StringField("external-resource-c1z",
		WithDescription("The path to the c1z file to sync external baton resources with"),
		WithPersistent(true),
		WithExportTarget(ExportTargetNone))
	externalResourceEntitlementIdFilter = StringField("external-resource-entitlement-id-filter",
		WithDescription("The entitlement that external users, groups must have access to sync external baton resources"),
		WithPersistent(true),
		WithExportTarget(ExportTargetNone))

	LambdaServerClientIDField = StringField("lambda-client-id", WithRequired(true), WithDescription("The oauth client id to use with the configuration endpoint"),
		WithExportTarget(ExportTargetNone))
	LambdaServerClientSecretField = StringField("lambda-client-secret", WithRequired(true), WithDescription("The oauth client secret to use with the configuration endpoint"),
		WithExportTarget(ExportTargetNone))

	// JWT Authentication Fields.
	LambdaServerAuthJWTSigner = StringField("lambda-auth-jwt-signer",
		WithRequired(false),
		WithDescription("The JWK format public key used to verify JWT signatures (mutually exclusive with lambda-auth-jwt-jwks-url)"),
		WithExportTarget(ExportTargetNone))

	LambdaServerAuthJWTJWKSUrl = StringField("lambda-auth-jwt-jwks-url",
		WithRequired(false),
		WithDescription("The URL to the JWKS endpoint for JWT verification (mutually exclusive with lambda-auth-jwt-signer)"),
		WithExportTarget(ExportTargetNone))

	LambdaServerAuthJWTExpectedIssuerField = StringField("lambda-auth-jwt-expected-issuer",
		WithRequired(true),
		WithDescription("The expected issuer claim in the JWT"),
		WithExportTarget(ExportTargetNone))

	LambdaServerAuthJWTExpectedSubjectField = StringField("lambda-auth-jwt-expected-subject",
		WithRequired(true),
		WithDescription("The expected subject claim in the JWT (optional)"),
		WithExportTarget(ExportTargetNone))

	LambdaServerAuthJWTExpectedAudienceField = StringField("lambda-auth-jwt-expected-audience",
		WithRequired(true),
		WithDescription("The expected audience claim in the JWT (optional)"),
		WithExportTarget(ExportTargetNone))
)

func LambdaServerFields() []SchemaField {
	return []SchemaField{
		LambdaServerClientIDField,
		LambdaServerClientSecretField,
		LambdaServerAuthJWTSigner,
		LambdaServerAuthJWTJWKSUrl,
		LambdaServerAuthJWTExpectedIssuerField,
		LambdaServerAuthJWTExpectedSubjectField,
		LambdaServerAuthJWTExpectedAudienceField,
	}
}

var LambdaServerRelationships = make([]SchemaFieldRelationship, 0)

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
	externalResourceC1ZField,
	externalResourceEntitlementIdFilter,

	otelCollectorEndpoint,
	otelCollectorEndpointTLSCertPath,
	otelCollectorEndpointTlSCert,
	otelCollectorEndpointTlSInsecure,
	otelTracingDisabled,
	otelLoggingDisabled,
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
