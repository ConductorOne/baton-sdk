package tasks

type TaskType uint8

func (tt TaskType) String() string {
	switch tt {
	case FullSyncType:
		return "sync_full"
	case GrantType:
		return "grant"
	case RevokeType:
		return "revoke"
	case HelloType:
		return "hello"
	case EventFeedType:
		return "event_feed"
	case NoneType:
		return "none"
	case CreateAccountType:
		return "create_account"
	case CreateResourceType:
		return "create_resource"
	case DeleteResourceType:
		return "delete_resource"
	case RotateCredentialsType:
		return "rotate_credential"
	case CreateTicketType:
		return "create_ticket"
	case ListTicketSchemasType:
		return "list_ticket_schemas"
	case GetTicketType:
		return "get_ticket"
	case GetTicketSchemaType:
		return "get_ticket_schema"
	case ListResourceTypesType:
		return "list_resource_types"
	case ListResourcesType:
		return "list_resources"
	case ListEntitlementsType:
		return "list_entitlements"
	case ListGrantsType:
		return "list_grants"
	case GetMetadataType:
		return "get_metadata"
	case ListEventsType:
		return "list_events"
	case StartDebugging:
		return "set_log_file_event"
	case BulkCreateTicketsType:
		return "bulk_create_tickets"
	case BulkGetTicketsType:
		return "bulk_get_tickets"
	case ActionListSchemasType:
		return "list_action_schemas"
	case ActionGetSchemaType:
		return "get_action_schema"
	case ActionInvokeType:
		return "invoke_action"
	case ActionStatusType:
		return "action_status"
	default:
		return "unknown"
	}
}

const (
	UnknownType TaskType = iota
	NoneType
	FullSyncType
	GrantType
	RevokeType
	HelloType
	CreateAccountType
	CreateResourceType
	DeleteResourceType
	RotateCredentialsType
	EventFeedType
	CreateTicketType
	ListTicketSchemasType
	GetTicketType
	GetTicketSchemaType
	ListResourceTypesType
	ListResourcesType
	ListEntitlementsType
	ListGrantsType
	GetMetadataType
	ListEventsType
	StartDebugging
	BulkCreateTicketsType
	BulkGetTicketsType
	ActionListSchemasType
	ActionGetSchemaType
	ActionInvokeType
	ActionStatusType
)
