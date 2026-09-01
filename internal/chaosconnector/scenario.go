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

	// SourceCache* declare per-serve-scope source-cache behavior, keyed by
	// the SAME keys as the page maps above (see SourceCacheSpec). Scopes
	// without a spec never consult the lookup.
	SourceCacheResources    map[string]*SourceCacheSpec
	SourceCacheEntitlements map[string]*SourceCacheSpec
	SourceCacheGrants       map[string]*SourceCacheSpec
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
		if err := validateSourceCacheSpecs(name, "resources", dataset.SourceCacheResources, dataset.Resources); err != nil {
			return err
		}
		if err := validateSourceCacheSpecs(name, "entitlements", dataset.SourceCacheEntitlements, dataset.Entitlements); err != nil {
			return err
		}
		if err := validateSourceCacheSpecs(name, "grants", dataset.SourceCacheGrants, dataset.Grants); err != nil {
			return err
		}
	}
	return nil
}

// validateSourceCacheSpecs fails fast on specs that would silently serve
// nothing: an unkeyed spec, a spec for a scope without pages, or a warm
// root token that no page declares.
func validateSourceCacheSpecs[T any](
	epoch, kind string,
	specs map[string]*SourceCacheSpec,
	pages map[string]Pages[T],
) error {
	for scope, spec := range specs {
		if spec == nil {
			return fmt.Errorf("chaosconnector: epoch %q %s source-cache spec for scope %q is nil", epoch, kind, scope)
		}
		if spec.ScopeKey == "" {
			return fmt.Errorf("chaosconnector: epoch %q %s source-cache spec for scope %q has no scope key", epoch, kind, scope)
		}
		scopePages, ok := pages[scope]
		if !ok {
			return fmt.Errorf("chaosconnector: epoch %q %s source-cache spec for scope %q has no pages", epoch, kind, scope)
		}
		if spec.WarmRoot != "" {
			if _, ok := scopePages[spec.WarmRoot]; !ok {
				return fmt.Errorf("chaosconnector: epoch %q %s source-cache spec for scope %q names warm root %q which no page declares",
					epoch, kind, scope, spec.WarmRoot)
			}
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

func cloneScenario(scenario *Scenario) *Scenario {
	if scenario == nil {
		return nil
	}
	out := &Scenario{
		Name:         scenario.Name,
		Seed:         scenario.Seed,
		InitialEpoch: scenario.InitialEpoch,
		Epochs:       make(map[string]*Dataset, len(scenario.Epochs)),
	}
	for epoch, dataset := range scenario.Epochs {
		out.Epochs[epoch] = cloneDataset(dataset)
	}
	return out
}

func cloneDataset(dataset *Dataset) *Dataset {
	if dataset == nil {
		return nil
	}
	return &Dataset{
		ResourceTypes:      cloneMessages(dataset.ResourceTypes),
		Resources:          clonePageMap(dataset.Resources),
		StaticEntitlements: clonePageMap(dataset.StaticEntitlements),
		Entitlements:       clonePageMap(dataset.Entitlements),
		Grants:             clonePageMap(dataset.Grants),

		SourceCacheResources:    cloneSourceCacheSpecs(dataset.SourceCacheResources),
		SourceCacheEntitlements: cloneSourceCacheSpecs(dataset.SourceCacheEntitlements),
		SourceCacheGrants:       cloneSourceCacheSpecs(dataset.SourceCacheGrants),
	}
}

func clonePageMap[T proto.Message](in map[string]Pages[T]) map[string]Pages[T] {
	out := make(map[string]Pages[T], len(in))
	for scope, pages := range in {
		clonedPages := make(Pages[T], len(pages))
		for token, page := range pages {
			page.List = cloneMessages(page.List)
			page.Spawn = append([]string(nil), page.Spawn...)
			page.Annotations = cloneMessages(page.Annotations)
			clonedPages[token] = page
		}
		out[scope] = clonedPages
	}
	return out
}
