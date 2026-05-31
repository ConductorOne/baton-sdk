package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

// AgentTraitOption is a functional option for configuring an AgentTrait.
//
// An agent is an autonomous non-human actor (e.g. an AI agent) that holds an
// identity.
type AgentTraitOption func(*v2.AgentTrait) error

// WithAgentStatus sets the agent's lifecycle status.
func WithAgentStatus(status v2.AgentTrait_AgentStatus) AgentTraitOption {
	return func(t *v2.AgentTrait) error {
		t.SetStatus(status)
		return nil
	}
}

// WithAgentIdentityResourceID sets the identity resource the agent
// authenticates as.
func WithAgentIdentityResourceID(identityResourceID *v2.ResourceId) AgentTraitOption {
	return func(t *v2.AgentTrait) error {
		t.SetIdentityResourceId(identityResourceID)
		return nil
	}
}

// WithAgentProfile sets the agent's profile struct.
func WithAgentProfile(profile map[string]interface{}) AgentTraitOption {
	return func(t *v2.AgentTrait) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}
		t.SetProfile(p)
		return nil
	}
}

// NewAgentTrait builds an AgentTrait from the given options.
func NewAgentTrait(opts ...AgentTraitOption) (*v2.AgentTrait, error) {
	trait := &v2.AgentTrait{}

	for _, opt := range opts {
		if err := opt(trait); err != nil {
			return nil, err
		}
	}

	return trait, nil
}

// GetAgentTrait returns the AgentTrait from a resource's annotations.
func GetAgentTrait(resource *v2.Resource) (*v2.AgentTrait, error) {
	ret := &v2.AgentTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("agent trait was not found on resource")
	}
	return ret, nil
}

// WithAgentTrait adds or updates an AgentTrait annotation on a resource.
func WithAgentTrait(opts ...AgentTraitOption) ResourceOption {
	return func(r *v2.Resource) error {
		t := &v2.AgentTrait{}

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
