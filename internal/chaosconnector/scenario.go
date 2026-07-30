package chaosconnector

import (
	"errors"
	"fmt"
	"reflect"
	"slices"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Page is one deterministic page in a cursor graph.
type Page[T any] struct {
	List        []T          `json:"-"`
	Next        string       `json:"next,omitempty"`
	Spawn       []string     `json:"spawn,omitempty"`
	Annotations []*anypb.Any `json:"-"`
}

// Pages maps opaque page tokens to responses. The empty token is the root
// request. Tokens need not form a tree: adversarial scenarios may deliberately
// contain convergence, re-mentions, or cycles.
type Pages[T any] map[string]Page[T]

// Dataset is the connector-visible data in one temporal epoch.
type Dataset struct {
	ResourceTypes      []*v2.ResourceType
	Resources          map[string]Pages[*v2.Resource]
	StaticEntitlements map[string]Pages[*v2.Entitlement]
	Entitlements       map[string]Pages[*v2.Entitlement]
	Grants             map[string]Pages[*v2.Grant]
}

// Scenario is an immutable deterministic connector world. Runtime state such
// as attempts, mutations, and the active epoch belongs to a Run.
type Scenario struct {
	Name         string
	Seed         int64
	InitialEpoch string
	Epochs       map[string]*Dataset
}

// Validate checks premises needed by every scenario adapter and oracle.
func (s *Scenario) Validate() error {
	if s == nil {
		return errors.New("chaosconnector: nil scenario")
	}
	if s.Name == "" {
		return errors.New("chaosconnector: scenario has no name")
	}
	if s.InitialEpoch == "" {
		return errors.New("chaosconnector: scenario has no initial epoch")
	}
	if len(s.Epochs) == 0 {
		return errors.New("chaosconnector: scenario has no epochs")
	}
	initial, ok := s.Epochs[s.InitialEpoch]
	if !ok {
		return fmt.Errorf("chaosconnector: initial epoch %q does not exist", s.InitialEpoch)
	}
	if initial == nil {
		return fmt.Errorf("chaosconnector: initial epoch %q is nil", s.InitialEpoch)
	}
	for name, dataset := range s.Epochs {
		if dataset == nil {
			return fmt.Errorf("chaosconnector: epoch %q is nil", name)
		}
		seenTypes := make(map[string]struct{}, len(dataset.ResourceTypes))
		for i, resourceType := range dataset.ResourceTypes {
			if resourceType == nil || resourceType.GetId() == "" {
				return fmt.Errorf("chaosconnector: epoch %q resource type %d has no id", name, i)
			}
			if _, exists := seenTypes[resourceType.GetId()]; exists {
				return fmt.Errorf("chaosconnector: epoch %q repeats resource type %q", name, resourceType.GetId())
			}
			seenTypes[resourceType.GetId()] = struct{}{}
		}
	}
	return nil
}

// Manifest is scenario-derived ground truth. It is cloned from the scenario so
// connector response mutation cannot modify the oracle.
type Manifest struct {
	Epoch              string
	ResourceTypes      []*v2.ResourceType
	Resources          []*v2.Resource
	StaticEntitlements []*v2.Entitlement
	Entitlements       []*v2.Entitlement
	Grants             []*v2.Grant
}

// Manifest derives an isolated canonical inventory for one epoch. It includes
// every declared page payload, independent of whether a faulty traversal
// reaches that page.
func (s *Scenario) Manifest(epoch string) (*Manifest, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	dataset, ok := s.Epochs[epoch]
	if !ok {
		return nil, fmt.Errorf("chaosconnector: epoch %q does not exist", epoch)
	}
	out := &Manifest{Epoch: epoch}
	out.ResourceTypes = cloneMessages(dataset.ResourceTypes)
	out.Resources = flattenPages(dataset.Resources)
	out.StaticEntitlements = flattenPages(dataset.StaticEntitlements)
	out.Entitlements = flattenPages(dataset.Entitlements)
	out.Grants = flattenPages(dataset.Grants)
	return out, nil
}

func flattenPages[T proto.Message](byScope map[string]Pages[T]) []T {
	scopes := make([]string, 0, len(byScope))
	for scope := range byScope {
		scopes = append(scopes, scope)
	}
	slices.Sort(scopes)

	var out []T
	for _, scope := range scopes {
		pages := byScope[scope]
		tokens := make([]string, 0, len(pages))
		for token := range pages {
			tokens = append(tokens, token)
		}
		slices.Sort(tokens)
		for _, token := range tokens {
			out = append(out, cloneMessages(pages[token].List)...)
		}
	}
	return out
}

func cloneMessages[T proto.Message](in []T) []T {
	out := make([]T, 0, len(in))
	for _, item := range in {
		value := reflect.ValueOf(item)
		if !value.IsValid() || (value.Kind() == reflect.Pointer && value.IsNil()) {
			out = append(out, item)
			continue
		}
		cloned, ok := proto.Clone(item).(T)
		if !ok {
			panic("chaosconnector: proto clone changed concrete type")
		}
		out = append(out, cloned)
	}
	return out
}
