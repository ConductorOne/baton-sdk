package field

var DefaultRelationships = []SchemaFieldRelationship{
	FieldsRequiredTogether(grantEntitlementField, grantPrincipalField),
	FieldsRequiredTogether(clientIDField, clientSecretField),
	FieldsRequiredTogether(createTicketField, ticketTemplatePathField),
	FieldsRequiredTogether(bulkCreateTicketField, bulkTicketTemplatePathField),
	FieldsRequiredTogether(getTicketField, ticketIDField),
	FieldsMutuallyExclusive(
		grantEntitlementField,
		revokeGrantField,
		createAccountLoginField,
		deleteResourceField,
		rotateCredentialsField,
		eventFeedField,
		createTicketField,
		getTicketField,
		ListTicketSchemasField,
	),
	FieldsMutuallyExclusive(
		grantEntitlementField,
		revokeGrantField,
		createAccountEmailField,
		deleteResourceTypeField,
		rotateCredentialsTypeField,
		eventFeedField,
		ListTicketSchemasField,
	),
}

// TODO(lauren) add mutually exclusive

func EnsureDefaultRelationships(original []SchemaFieldRelationship) []SchemaFieldRelationship {
	return append(DefaultRelationships, original...)
}
