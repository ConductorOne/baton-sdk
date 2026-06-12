package output

import (
	"strings"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/baton/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDropUnresolvableAnyMessages(t *testing.T) {
	known, err := anypb.New(v2.ExternalLink_builder{Url: "https://example.com"}.Build())
	require.NoError(t, err)
	unknown := &anypb.Any{
		TypeUrl: "type.googleapis.com/c1.connector.v2.RemovedAnnotation",
		Value:   []byte{0x0a, 0x03, 'o', 'l', 'd'},
	}

	in := &v1.GrantListOutput{
		Grants: []*v1.GrantOutput{
			{
				Grant: v2.Grant_builder{
					Id:          "grant-1",
					Annotations: []*anypb.Any{unknown, known},
				}.Build(),
			},
		},
	}

	_, err = protojson.Marshal(in)
	require.Error(t, err)

	out, err := protojson.Marshal(dropUnresolvableAnyMessages(in))
	require.NoError(t, err)
	require.NotContains(t, string(out), "RemovedAnnotation")
	require.True(t, strings.Contains(string(out), "ExternalLink"))
	require.Len(t, in.GetGrants()[0].GetGrant().GetAnnotations(), 2, "input must not be mutated")
}
