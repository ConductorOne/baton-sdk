package field

var defaultRelationship = []SchemaFieldRelationship{
	FieldsRequiredTogether(grantEntitlementField, grantPrincipalField),
	FieldsRequiredTogether(clientIDField, clientSecretField),
	FieldsRequiredTogether(createTicketField, ticketTemplatePathField),
	FieldsRequiredTogether(getTicketField, ticketIDField),
	FieldsMutuallyExclusive(fileField, clientIDField),
	FieldsMutuallyExclusive(
		grantEntitlementField,
		revokeGrantField,
		createAccountLoginField,
		deleteResourceField,
		rotateCredentialsField,
		eventFeedField,
		createTicketField,
		getTicketField,
		listTicketSchemasField,
	),
	FieldsMutuallyExclusive(
		grantEntitlementField,
		revokeGrantField,
		createAccountEmailField,
		deleteResourceTypeField,
		rotateCredentialsTypeField,
		eventFeedField,
		listTicketSchemasField,
	),
}

func EnsureDefaultRelationships(original []SchemaFieldRelationship) []SchemaFieldRelationship {
	return append(defaultRelationship, original...)
}
