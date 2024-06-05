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
)
