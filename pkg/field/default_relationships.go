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
		bulkCreateTicketField,
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
	FieldsDependentOn(
		[]SchemaField{externalResourceEntitlementIdFilter},
		[]SchemaField{externalResourceC1ZField},
	),
}

func EnsureDefaultRelationships(original []SchemaFieldRelationship) []SchemaFieldRelationship {
	return append(DefaultRelationships, original...)
}
