package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTaskTypeOrdinalsPinned pins every TaskType constant to its released
// numeric value. The iota block in tasks.go holds a blank placeholder at 30
// where CreateSyncDiff was removed; deleting that placeholder — or inserting
// or reordering entries — silently renumbers every later constant. A comment
// is the only other thing preventing that, so this test turns the mistake
// into a loud failure instead of a silent shift.
func TestTaskTypeOrdinalsPinned(t *testing.T) {
	pinned := []struct {
		name string
		got  TaskType
		want TaskType
	}{
		{"UnknownType", UnknownType, 0},
		{"NoneType", NoneType, 1},
		{"FullSyncType", FullSyncType, 2},
		{"GrantType", GrantType, 3},
		{"RevokeType", RevokeType, 4},
		{"HelloType", HelloType, 5},
		{"CreateAccountType", CreateAccountType, 6},
		{"CreateResourceType", CreateResourceType, 7},
		{"DeleteResourceType", DeleteResourceType, 8},
		{"RotateCredentialsType", RotateCredentialsType, 9},
		{"EventFeedType", EventFeedType, 10},
		{"CreateTicketType", CreateTicketType, 11},
		{"ListTicketSchemasType", ListTicketSchemasType, 12},
		{"GetTicketType", GetTicketType, 13},
		{"GetTicketSchemaType", GetTicketSchemaType, 14},
		{"ListResourceTypesType", ListResourceTypesType, 15},
		{"ListResourcesType", ListResourcesType, 16},
		{"GetResourceType", GetResourceType, 17},
		{"ListEntitlementsType", ListEntitlementsType, 18},
		{"ListGrantsType", ListGrantsType, 19},
		{"GetMetadataType", GetMetadataType, 20},
		{"ListEventsType", ListEventsType, 21},
		{"ListEventFeedsType", ListEventFeedsType, 22},
		{"StartDebugging", StartDebugging, 23},
		{"BulkCreateTicketsType", BulkCreateTicketsType, 24},
		{"BulkGetTicketsType", BulkGetTicketsType, 25},
		{"ActionListSchemasType", ActionListSchemasType, 26},
		{"ActionGetSchemaType", ActionGetSchemaType, 27},
		{"ActionInvokeType", ActionInvokeType, 28},
		{"ActionStatusType", ActionStatusType, 29},
		// 30 was CreateSyncDiff; the blank identifier in the const
		// block holds its slot so the two below keep released values.
		{"ListStaticEntitlementsType", ListStaticEntitlementsType, 31},
		{"IssueCredentialType", IssueCredentialType, 32},
	}
	for _, p := range pinned {
		require.Equal(t, p.want, p.got, "TaskType %s must keep its released ordinal", p.name)
	}
}
