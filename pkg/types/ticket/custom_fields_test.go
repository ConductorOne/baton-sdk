package ticket

import (
	"context"
	"reflect"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

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
