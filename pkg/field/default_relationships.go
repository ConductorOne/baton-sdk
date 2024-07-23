package field

var defaultRelationship = []SchemaFieldRelationship{
	FieldsRequiredTogether(grantEntitlementField, grantPrincipalField),
	FieldsRequiredTogether(clientIDField, clientSecretField),
	FieldsRequiredTogether(createTicketField, ticketTemplatePathField),
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

func EnsureDefaultRelationships(original []SchemaFieldRelationship) []SchemaFieldRelationship {
	return append(defaultRelationship, original...)
}
