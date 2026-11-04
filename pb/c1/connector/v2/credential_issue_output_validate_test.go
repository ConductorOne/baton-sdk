package v2

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
)

func TestCredentialIssueOptionDescriptorOutputRoundTrip(t *testing.T) {
	descriptor := CredentialIssueOptionDescriptor_builder{
		Option:               CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		ResourceMode:         CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		SecretResourceTypeId: "organization-api-key",
		Output: config.Field_builder{
			ContentType: "application/vnd.multipass.api_key_v2+json",
			Tags:        []string{"credential", "multipass", "api_key_v2"},
			StringField: config.StringField_builder{}.Build(),
		}.Build(),
	}.Build()
	require.NoError(t, descriptor.Validate())

	encoded, err := proto.Marshal(descriptor)
	require.NoError(t, err)
	decoded := &CredentialIssueOptionDescriptor{}
	require.NoError(t, proto.Unmarshal(encoded, decoded))

	require.True(t, proto.Equal(descriptor, decoded))
	require.True(t, decoded.GetOutput().HasStringField(), "the oneof must survive the wire round trip")
	require.Equal(t, "application/vnd.multipass.api_key_v2+json", decoded.GetOutput().GetContentType())
	require.Equal(t, []string{"credential", "multipass", "api_key_v2"}, decoded.GetOutput().GetTags())
}

func TestCredentialIssueOptionDescriptorOutputRejectsInvalidContentType(t *testing.T) {
	descriptor := CredentialIssueOptionDescriptor_builder{
		SecretResourceTypeId: "organization-api-key",
		Output:               config.Field_builder{ContentType: "nope"}.Build(),
	}.Build()
	require.Error(t, descriptor.Validate())
}
