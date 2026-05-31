package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

// AgentToolTraitOption is a functional option for configuring an
// AgentToolTrait.
//
// An agent tool is a capability an agent can invoke: a callable tool or a
// knowledge base. The platform-specific kind is carried in
// profile.tool_kind_detail, not in the AgentToolKind enum.
type AgentToolTraitOption func(*v2.AgentToolTrait) error

// WithAgentToolKind sets the kind of the agent tool.
func WithAgentToolKind(toolKind v2.AgentToolTrait_AgentToolKind) AgentToolTraitOption {
	return func(t *v2.AgentToolTrait) error {
		t.SetToolKind(toolKind)
		return nil
	}
}

// WithAgentToolCredentialProviderRef sets the credential provider the tool
// authenticates through.
func WithAgentToolCredentialProviderRef(credentialProviderRef *v2.ResourceId) AgentToolTraitOption {
	return func(t *v2.AgentToolTrait) error {
		t.SetCredentialProviderRef(credentialProviderRef)
		return nil
	}
}

// WithAgentToolOwningResourceRef sets the resource that owns/exposes the tool.
func WithAgentToolOwningResourceRef(owningResourceRef *v2.ResourceId) AgentToolTraitOption {
	return func(t *v2.AgentToolTrait) error {
		t.SetOwningResourceRef(owningResourceRef)
		return nil
	}
}

// WithAgentToolProfile sets the agent tool's profile struct.
func WithAgentToolProfile(profile map[string]interface{}) AgentToolTraitOption {
	return func(t *v2.AgentToolTrait) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}
		t.SetProfile(p)
		return nil
	}
}

// NewAgentToolTrait builds an AgentToolTrait from the given options.
func NewAgentToolTrait(opts ...AgentToolTraitOption) (*v2.AgentToolTrait, error) {
	trait := &v2.AgentToolTrait{}

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	return trait, nil
}

// GetAgentToolTrait returns the AgentToolTrait from a resource's annotations.
func GetAgentToolTrait(resource *v2.Resource) (*v2.AgentToolTrait, error) {
	ret := &v2.AgentToolTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("agent tool trait was not found on resource")
	}
	return ret, nil
}

// WithAgentToolTrait adds or updates an AgentToolTrait annotation on a
// resource.
func WithAgentToolTrait(opts ...AgentToolTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.AgentToolTrait{}

		annos := annotations.Annotations(r.GetAnnotations())
		_, err := annos.Pick(t)
		if err != nil {
			return err
		}

		for _, o := range opts {
			if err := o(t); err != nil {
				return err
			}
		}

		annos.Update(t)
		r.SetAnnotations(annos)
		return nil
	}
}
