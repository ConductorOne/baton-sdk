package ticket

import (
	"reflect"
	"testing"

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
				id: "component",
				values: []*v2.TicketCustomFieldObjectValue{
					{
						Id: "1",
					},
				},
			},
			want: &v2.TicketCustomField{
				Id:   "component",
				Value: &v2.{
					PickMultipleObjectValues: &v2.TicketCustomFieldPickMultipleObjectValues{
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
