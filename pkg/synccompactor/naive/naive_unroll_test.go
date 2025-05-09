package naive

import (
	"reflect"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func Test_createRequest(t *testing.T) {
	type testCase[REQ listRequest] struct {
		name string
		want REQ
	}
	tests := []testCase[*v2.GrantsServiceListGrantsRequest]{
		{
			name: "empty",
			want: &v2.GrantsServiceListGrantsRequest{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createRequest[*v2.GrantsServiceListGrantsRequest](); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
