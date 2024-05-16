package ticket

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestValidateTicket(t *testing.T) {
	now := time.Now()
	type args struct {
		ctx    context.Context
		schema *v2.TicketSchema
		ticket *v2.Ticket
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "TestValidateTicketValidProject1",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueFieldSchema("project",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueField("project", &v2.TicketCustomFieldObjectValue{Id: "10000"}),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketValidProject2",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueFieldSchema("project",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueField("project", &v2.TicketCustomFieldObjectValue{Id: "10001"}),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketInvalidProject",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueFieldSchema("project",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueField("project", &v2.TicketCustomFieldObjectValue{Id: "10002"}),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickObjectNotRequiredInvalid",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"pick_object": PickObjectValueFieldSchema("pick_object",
							"",
							false, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"pick_object": PickObjectValueField("pick_object", &v2.TicketCustomFieldObjectValue{Id: "10002"}),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketInvalidType",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueFieldSchema("project",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10002"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueField("project", &v2.TicketCustomFieldObjectValue{Id: "10001"}),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketInvalidStatus",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueFieldSchema("project",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10002"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"project": PickObjectValueField("project", &v2.TicketCustomFieldObjectValue{Id: "10001"}),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketValidMultiple",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesFieldSchema("custom",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
								{
									Id: "10003",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesField("custom", []*v2.TicketCustomFieldObjectValue{
							{
								Id: "10000",
							},
							{
								Id: "10001",
							},
						},
						),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketInvalidMultiple",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesFieldSchema("custom",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
								{
									Id: "10003",
								},
							},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesField("custom", []*v2.TicketCustomFieldObjectValue{
							{
								Id: "10000",
							},
							{
								Id: "10004",
							},
						},
						),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketMultipleCustomInvalidMultipleValidString",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesFieldSchema("custom",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
								{
									Id: "10003",
								},
							},
						),
						"custom_string": StringsFieldSchema("custom", "", true),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesField("custom", []*v2.TicketCustomFieldObjectValue{
							{
								Id: "10000",
							},
							{
								Id: "10004",
							},
						},
						),
						"custom_string": StringField("custom_string", "somestring"),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketMultipleCustomValidMultipleRequiredStringNotPresent",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesFieldSchema("custom",
							"",
							true, []*v2.TicketCustomFieldObjectValue{
								{
									Id: "10000",
								},
								{
									Id: "10001",
								},
								{
									Id: "10003",
								},
							},
						),
						"custom_string": StringsFieldSchema("custom", "", true),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom": PickMultipleObjectValuesField("custom", []*v2.TicketCustomFieldObjectValue{
							{
								Id: "10000",
							},
							{
								Id: "10001",
							},
						},
						),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketValidBool",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_bool": BoolFieldSchema("custom_bool",
							"",
							true,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_bool": BoolField("custom_bool", true),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketBoolNotRequired",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_bool": BoolFieldSchema("custom_bool",
							"",
							false,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:           "10043",
					DisplayName:  "Test Ticket",
					Description:  "",
					Assignees:    nil,
					Reporter:     nil,
					Status:       &v2.TicketStatus{Id: "10001"},
					Type:         &v2.TicketType{Id: "10001"},
					Labels:       []string{"test", "baton", "api"},
					Url:          "",
					CustomFields: map[string]*v2.TicketCustomField{},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketValidString",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": StringFieldSchema("custom_string",
							"",
							true,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": StringField("custom_string", "somestring"),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketStringRequiredNotPresent",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": StringFieldSchema("custom_string",
							"",
							true,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:           "10043",
					DisplayName:  "Test Ticket",
					Description:  "",
					Assignees:    nil,
					Reporter:     nil,
					Status:       &v2.TicketStatus{Id: "10001"},
					Type:         &v2.TicketType{Id: "10001"},
					Labels:       []string{"test", "baton", "api"},
					Url:          "",
					CustomFields: map[string]*v2.TicketCustomField{},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketStringNotRequired",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": StringFieldSchema("custom_string",
							"",
							false,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": StringField("custom_string", "somestring"),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketStrings",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_strings": StringsFieldSchema("custom_strings",
							"",
							true,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_strings": StringsField("custom_strings", []string{"somestring", "somestring2"}),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketStringsRequiredNotPresent",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_strings": StringsFieldSchema("custom_strings",
							"",
							true,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:           "10043",
					DisplayName:  "Test Ticket",
					Description:  "",
					Assignees:    nil,
					Reporter:     nil,
					Status:       &v2.TicketStatus{Id: "10001"},
					Type:         &v2.TicketType{Id: "10001"},
					Labels:       []string{"test", "baton", "api"},
					Url:          "",
					CustomFields: map[string]*v2.TicketCustomField{},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketStringsNotRequired",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_strings": StringsFieldSchema("custom_strings",
							"",
							false,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_strings": StringsField("custom_strings", []string{"somestring", "somestring2"}),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketTimeRequired",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_time": TimestampFieldSchema("custom_time",
							"",
							true,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_time": TimestampField("10000", now),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketTimeRequiredNotPresent",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_time": TimestampFieldSchema("custom_time",
							"",
							true,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:           "10043",
					DisplayName:  "Test Ticket",
					Description:  "",
					Assignees:    nil,
					Reporter:     nil,
					Status:       &v2.TicketStatus{Id: "10001"},
					Type:         &v2.TicketType{Id: "10001"},
					Labels:       []string{"test", "baton", "api"},
					Url:          "",
					CustomFields: map[string]*v2.TicketCustomField{},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketTimeNotRequired",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_time": TimestampFieldSchema("custom_time",
							"",
							false,
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_time": TimestampField("10000", now),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickString",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringFieldSchema("custom_string",
							"",
							true, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringField("custom_string", "allowed1"),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickStringInvalid",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringFieldSchema("custom_string",
							"",
							true, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringField("custom_string", "notallowed"),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickStringNotRequired",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringFieldSchema("custom_string",
							"",
							false, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringField("custom_string", "allowed1"),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickStringNotRequiredInvalid",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringFieldSchema("custom_string",
							"",
							false, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickStringField("custom_string", "notallowed"),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickStringsRequiredValid",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsFieldSchema("custom_string",
							"",
							true, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsField("custom_string", []string{"allowed1", "allowed2"}),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickStringsNotRequiredValid",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsFieldSchema("custom_string",
							"",
							false, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsField("custom_string", []string{"allowed1", "allowed2"}),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickStringsRequiredInvalid",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsFieldSchema("custom_string",
							"",
							true, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsField("custom_string", []string{"allowed1", "notallowed"}),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "TestValidateTicketPickStringsNotRequiredInvalid",
			args: args{
				ctx: context.TODO(),
				schema: &v2.TicketSchema{
					Id:          "10001",
					DisplayName: "",
					Types: []*v2.TicketType{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					Statuses: []*v2.TicketStatus{
						{
							Id:          "10000",
							DisplayName: "",
						},
						{
							Id:          "10001",
							DisplayName: "",
						},
					},
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsFieldSchema("custom_string",
							"",
							false, []string{"allowed1", "allowed2", "allowed3"},
						),
					},
				},
				ticket: &v2.Ticket{
					Id:          "10043",
					DisplayName: "Test Ticket",
					Description: "",
					Assignees:   nil,
					Reporter:    nil,
					Status:      &v2.TicketStatus{Id: "10001"},
					Type:        &v2.TicketType{Id: "10001"},
					Labels:      []string{"test", "baton", "api"},
					Url:         "",
					CustomFields: map[string]*v2.TicketCustomField{
						"custom_string": PickMultipleStringsField("custom_string", []string{"allowed1", "notallowed"}),
					},
				},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateTicket(tt.args.ctx, tt.args.schema, tt.args.ticket)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTicket() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ValidateTicket() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringFieldSchema(t *testing.T) {
	type args struct {
		id          string
		displayName string
		required    bool
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test StringFieldSchema",
			args: args{
				id:          "component",
				displayName: "Component",
				required:    true,
			},
			want: &v2.TicketCustomField{
				Id:          "component",
				DisplayName: "Component",
				Required:    true,
				Value: &v2.TicketCustomField_StringValue{
					StringValue: &v2.TicketCustomFieldStringValue{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringFieldSchema(tt.args.id, tt.args.displayName, tt.args.required); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringField(t *testing.T) {
	type args struct {
		id    string
		value string
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test StringField",
			args: args{
				id:    "component",
				value: "test string",
			},
			want: &v2.TicketCustomField{
				Id: "component",
				Value: &v2.TicketCustomField_StringValue{
					StringValue: &v2.TicketCustomFieldStringValue{
						Value: "test string",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringField(tt.args.id, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringsFieldSchema(t *testing.T) {
	type args struct {
		id          string
		displayName string
		required    bool
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test StringsFieldSchema",
			args: args{
				id:          "components",
				displayName: "Components",
				required:    false,
			},
			want: &v2.TicketCustomField{
				Id:          "components",
				DisplayName: "Components",
				Required:    false,
				Value: &v2.TicketCustomField_StringValues{
					StringValues: &v2.TicketCustomFieldStringValues{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringsFieldSchema(tt.args.id, tt.args.displayName, tt.args.required); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringsFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringsField(t *testing.T) {
	type args struct {
		id     string
		values []string
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test StringsField",
			args: args{
				id:     "components",
				values: []string{"frontend", "backend"},
			},
			want: &v2.TicketCustomField{
				Id: "components",
				Value: &v2.TicketCustomField_StringValues{
					StringValues: &v2.TicketCustomFieldStringValues{
						Values: []string{"frontend", "backend"},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringsField(tt.args.id, tt.args.values); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringsField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoolFieldSchema(t *testing.T) {
	type args struct {
		id          string
		displayName string
		required    bool
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test BoolFieldSchema",
			args: args{
				id:          "is_active",
				displayName: "Is Active",
				required:    true,
			},
			want: &v2.TicketCustomField{
				Id:          "is_active",
				DisplayName: "Is Active",
				Required:    true,
				Value: &v2.TicketCustomField_BoolValue{
					BoolValue: &v2.TicketCustomFieldBoolValue{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BoolFieldSchema(tt.args.id, tt.args.displayName, tt.args.required); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BoolFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoolField(t *testing.T) {
	type args struct {
		id    string
		value bool
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test BoolField",
			args: args{
				id:    "is_active",
				value: true,
			},
			want: &v2.TicketCustomField{
				Id: "is_active",
				Value: &v2.TicketCustomField_BoolValue{
					BoolValue: &v2.TicketCustomFieldBoolValue{
						Value: true,
					},
				},
			},
		},
		{
			name: "Test BoolField",
			args: args{
				id:    "is_active",
				value: false,
			},
			want: &v2.TicketCustomField{
				Id: "is_active",
				Value: &v2.TicketCustomField_BoolValue{
					BoolValue: &v2.TicketCustomFieldBoolValue{
						Value: false,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BoolField(tt.args.id, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BoolField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestampFieldSchema(t *testing.T) {
	type args struct {
		id          string
		displayName string
		required    bool
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test TimestampFieldSchema",
			args: args{
				id:          "created_at",
				displayName: "Created At",
				required:    true,
			},
			want: &v2.TicketCustomField{
				Id:          "created_at",
				DisplayName: "Created At",
				Required:    true,
				Value: &v2.TicketCustomField_TimestampValue{
					TimestampValue: &v2.TicketCustomFieldTimestampValue{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TimestampFieldSchema(tt.args.id, tt.args.displayName, tt.args.required); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimestampFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestampField(t *testing.T) {
	now := time.Now()
	type args struct {
		id    string
		value time.Time
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test TimestampField",
			args: args{
				id:    "created_at",
				value: now,
			},
			want: &v2.TicketCustomField{
				Id: "created_at",
				Value: &v2.TicketCustomField_TimestampValue{
					TimestampValue: &v2.TicketCustomFieldTimestampValue{
						Value: timestamppb.New(now),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TimestampField(tt.args.id, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimestampField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickStringFieldSchema(t *testing.T) {
	type args struct {
		id            string
		displayName   string
		required      bool
		allowedValues []string
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickStringFieldSchema",
			args: args{
				id:            "priority",
				displayName:   "Priority",
				required:      true,
				allowedValues: []string{"low", "medium", "high"},
			},
			want: &v2.TicketCustomField{
				Id:          "priority",
				DisplayName: "Priority",
				Required:    true,
				Value: &v2.TicketCustomField_PickStringValue{
					PickStringValue: &v2.TicketCustomFieldPickStringValue{
						AllowedValues: []string{"low", "medium", "high"},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickStringFieldSchema(tt.args.id, tt.args.displayName, tt.args.required, tt.args.allowedValues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickStringFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickStringField(t *testing.T) {
	type args struct {
		id    string
		value string
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickStringField",
			args: args{
				id:    "priority",
				value: "high",
			},
			want: &v2.TicketCustomField{
				Id: "priority",
				Value: &v2.TicketCustomField_PickStringValue{
					PickStringValue: &v2.TicketCustomFieldPickStringValue{
						Value: "high",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickStringField(tt.args.id, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickStringField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickMultipleStringsFieldSchema(t *testing.T) {
	type args struct {
		id            string
		displayName   string
		required      bool
		allowedValues []string
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickMultipleStringsFieldSchema",
			args: args{
				id:            "pets",
				displayName:   "Pets",
				required:      false,
				allowedValues: []string{"dog", "cat", "fish"},
			},
			want: &v2.TicketCustomField{
				Id:          "pets",
				DisplayName: "Pets",
				Required:    false,
				Value: &v2.TicketCustomField_PickMultipleStringValues{
					PickMultipleStringValues: &v2.TicketCustomFieldPickMultipleStringValues{
						AllowedValues: []string{"dog", "cat", "fish"},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickMultipleStringsFieldSchema(tt.args.id, tt.args.displayName, tt.args.required, tt.args.allowedValues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickMultipleStringsFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickMultipleStringsField(t *testing.T) {
	type args struct {
		id     string
		values []string
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickMultipleStringsField",
			args: args{
				id:     "pets",
				values: []string{"dog", "cat"},
			},
			want: &v2.TicketCustomField{
				Id: "pets",
				Value: &v2.TicketCustomField_PickMultipleStringValues{
					PickMultipleStringValues: &v2.TicketCustomFieldPickMultipleStringValues{
						Values: []string{"dog", "cat"},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickMultipleStringsField(tt.args.id, tt.args.values); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickMultipleStringsField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickObjectValueFieldSchema(t *testing.T) {
	type args struct {
		id            string
		displayName   string
		required      bool
		allowedValues []*v2.TicketCustomFieldObjectValue
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickObjectValueFieldSchema",
			args: args{
				id:          "component",
				displayName: "Component",
				required:    true,
				allowedValues: []*v2.TicketCustomFieldObjectValue{
					{
						Id:          "1",
						DisplayName: "Frontend",
					},
					{
						Id:          "2",
						DisplayName: "Backend",
					},
				},
			},
			want: &v2.TicketCustomField{
				Id:          "component",
				DisplayName: "Component",
				Required:    true,
				Value: &v2.TicketCustomField_PickObjectValue{
					PickObjectValue: &v2.TicketCustomFieldPickObjectValue{
						AllowedValues: []*v2.TicketCustomFieldObjectValue{
							{
								Id:          "1",
								DisplayName: "Frontend",
							},
							{
								Id:          "2",
								DisplayName: "Backend",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickObjectValueFieldSchema(tt.args.id, tt.args.displayName, tt.args.required, tt.args.allowedValues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickObjectValueFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickObjectValueField(t *testing.T) {
	type args struct {
		id    string
		value *v2.TicketCustomFieldObjectValue
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickObjectValueField",
			args: args{
				id: "component",
				value: &v2.TicketCustomFieldObjectValue{
					Id: "1",
				},
			},
			want: &v2.TicketCustomField{
				Id: "component",
				Value: &v2.TicketCustomField_PickObjectValue{
					PickObjectValue: &v2.TicketCustomFieldPickObjectValue{
						Value: &v2.TicketCustomFieldObjectValue{
							Id: "1",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickObjectValueField(tt.args.id, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickObjectValueField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickMultipleObjectValuesFieldSchema(t *testing.T) {
	type args struct {
		id            string
		displayName   string
		required      bool
		allowedValues []*v2.TicketCustomFieldObjectValue
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickMultipleObjectValuesFieldSchema",
			args: args{
				id:          "components",
				displayName: "Components",
				required:    false,
				allowedValues: []*v2.TicketCustomFieldObjectValue{
					{
						Id:          "1",
						DisplayName: "Frontend",
					},
					{
						Id:          "2",
						DisplayName: "Backend",
					},
				},
			},
			want: &v2.TicketCustomField{
				Id:          "components",
				DisplayName: "Components",
				Required:    false,
				Value: &v2.TicketCustomField_PickMultipleObjectValues{
					PickMultipleObjectValues: &v2.TicketCustomFieldPickMultipleObjectValues{
						AllowedValues: []*v2.TicketCustomFieldObjectValue{
							{
								Id:          "1",
								DisplayName: "Frontend",
							},
							{
								Id:          "2",
								DisplayName: "Backend",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickMultipleObjectValuesFieldSchema(tt.args.id, tt.args.displayName, tt.args.required, tt.args.allowedValues); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickMultipleObjectValuesFieldSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickMultipleObjectValuesField(t *testing.T) {
	type args struct {
		id     string
		values []*v2.TicketCustomFieldObjectValue
	}
	tests := []struct {
		name string
		args args
		want *v2.TicketCustomField
	}{
		{
			name: "Test PickMultipleObjectValuesField",
			args: args{
				id: "components",
				values: []*v2.TicketCustomFieldObjectValue{
					{
						Id: "1",
					},
					{
						Id: "2",
					},
				},
			},
			want: &v2.TicketCustomField{
				Id: "components",
				Value: &v2.TicketCustomField_PickMultipleObjectValues{
					PickMultipleObjectValues: &v2.TicketCustomFieldPickMultipleObjectValues{
						Values: []*v2.TicketCustomFieldObjectValue{
							{
								Id: "1",
							},
							{
								Id: "2",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PickMultipleObjectValuesField(tt.args.id, tt.args.values); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PickMultipleObjectValuesField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetCustomFieldValue(t *testing.T) {
	now := time.Now()
	type args struct {
		field *v2.TicketCustomField
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Test StringValue",
			args: args{
				field: StringField("severity", "high"),
			},
			want:    "high",
			wantErr: false,
		},
		{
			name: "Test StringValues",
			args: args{
				field: StringsField("components", []string{"frontend", "backend"}),
			},
			want:    []string{"frontend", "backend"},
			wantErr: false,
		},
		{
			name: "Test BoolValue true",
			args: args{
				field: BoolField("is_active", true),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Test BoolValue false",
			args: args{
				field: BoolField("is_active", false),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "Test TimestampValue",
			args: args{
				field: TimestampField("created_at", now),
			},
			want:    timestamppb.New(now),
			wantErr: false,
		},
		{
			name: "Test PickStringValue",
			args: args{
				field: PickStringField("priority", "high"),
			},
			want: "high",
		},
		{
			name: "Test PickMultipleStringValues",
			args: args{
				field: PickMultipleStringsField("pets", []string{"dog", "cat"}),
			},
			want: []string{"dog", "cat"},
		},
		{
			name: "Test PickObjectValue",
			args: args{
				field: PickObjectValueField("component", &v2.TicketCustomFieldObjectValue{
					Id: "1",
				}),
			},
			want: &v2.TicketCustomFieldObjectValue{
				Id: "1",
			},
		},
		{
			name: "Test PickObjectValue",
			args: args{
				field: PickObjectValueField("component", &v2.TicketCustomFieldObjectValue{
					Id: "2",
				}),
			},
			want: &v2.TicketCustomFieldObjectValue{
				Id: "2",
			},
		},
		{
			name: "Test PickMultipleObjectValues",
			args: args{
				field: PickMultipleObjectValuesField("components", []*v2.TicketCustomFieldObjectValue{
					{
						Id: "1",
					},
					{
						Id: "2",
					},
				}),
			},
			want: []*v2.TicketCustomFieldObjectValue{
				{
					Id: "1",
				},
				{
					Id: "2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetCustomFieldValue(tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCustomFieldValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCustomFieldValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}
