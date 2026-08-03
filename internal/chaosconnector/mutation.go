package chaosconnector

import (
	"fmt"
	"sync"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	MutationUnknownAnnotation   = "annotation/unknown"
	MutationMalformedAnnotation = "annotation/malformed-known"
	MutationDuplicateAnnotation = "annotation/duplicate-first"
	MutationClearNextPageToken  = "response/clear-next-page-token"
	MutationUnknownProtoField   = "response/unknown-proto-field"
	MutationDuplicateFirstItem  = "response/duplicate-first-list-item"
	MutationReverseFirstList    = "response/reverse-first-list"
	MutationClearFirstItem      = "response/clear-first-list-item"
)

// Mutation transforms a response after the connector implementation returns.
type Mutation func(proto.Message) error

// MutationRegistry resolves replayable names to typed response transforms.
type MutationRegistry struct {
	mu        sync.RWMutex
	mutations map[string]Mutation
}

// NewMutationRegistry returns the built-in response mutation catalog.
func NewMutationRegistry() *MutationRegistry {
	registry := &MutationRegistry{mutations: make(map[string]Mutation)}
	registry.mustRegister(MutationUnknownAnnotation, appendUnknownAnnotation)
	registry.mustRegister(MutationMalformedAnnotation, appendMalformedAnnotation)
	registry.mustRegister(MutationDuplicateAnnotation, duplicateFirstAnnotation)
	registry.mustRegister(MutationClearNextPageToken, clearNextPageToken)
	registry.mustRegister(MutationUnknownProtoField, appendUnknownProtoField)
	registry.mustRegister(MutationDuplicateFirstItem, duplicateFirstListItem)
	registry.mustRegister(MutationReverseFirstList, reverseFirstList)
	registry.mustRegister(MutationClearFirstItem, clearFirstListItem)
	return registry
}

// Register adds a named mutation. Schedule files contain only this stable name.
func (r *MutationRegistry) Register(name string, mutation Mutation) error {
	if name == "" {
		return fmt.Errorf("chaosconnector: mutation has no name")
	}
	if mutation == nil {
		return fmt.Errorf("chaosconnector: mutation %q is nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.mutations[name]; exists {
		return fmt.Errorf("chaosconnector: mutation %q is already registered", name)
	}
	r.mutations[name] = mutation
	return nil
}

func (r *MutationRegistry) mustRegister(name string, mutation Mutation) {
	if err := r.Register(name, mutation); err != nil {
		panic(err)
	}
}

// Apply executes a registered mutation.
func (r *MutationRegistry) Apply(name string, response proto.Message) error {
	if response == nil {
		return fmt.Errorf("chaosconnector: mutation %q has no response", name)
	}
	r.mu.RLock()
	mutation, ok := r.mutations[name]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("chaosconnector: mutation %q is not registered", name)
	}
	before := proto.Clone(response)
	if err := mutation(response); err != nil {
		return fmt.Errorf("chaosconnector: apply mutation %q: %w", name, err)
	}
	if proto.Equal(before, response) {
		return fmt.Errorf("chaosconnector: mutation %q did not change the response", name)
	}
	return nil
}

// Names returns the registered replay vocabulary.
func (r *MutationRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.mutations))
	for name := range r.mutations {
		out = append(out, name)
	}
	return out
}

func appendUnknownAnnotation(response proto.Message) error {
	list, err := firstAnnotationList(response.ProtoReflect())
	if err != nil {
		return err
	}
	list.Append(protoreflect.ValueOfMessage((&anypb.Any{
		TypeUrl: "type.googleapis.com/chaosconnector.UnknownAnnotation",
		Value:   []byte{0x08, 0x01},
	}).ProtoReflect()))
	return nil
}

func appendMalformedAnnotation(response proto.Message) error {
	list, err := firstAnnotationList(response.ProtoReflect())
	if err != nil {
		return err
	}
	list.Append(protoreflect.ValueOfMessage((&anypb.Any{
		TypeUrl: "type.googleapis.com/" + string((&v2.EnqueuePageTokens{}).ProtoReflect().Descriptor().FullName()),
		Value:   []byte{0xff, 0xff},
	}).ProtoReflect()))
	return nil
}

func duplicateFirstAnnotation(response proto.Message) error {
	list, err := firstAnnotationList(response.ProtoReflect())
	if err != nil {
		return err
	}
	if list.Len() == 0 {
		return fmt.Errorf("response has no annotation to duplicate")
	}
	original := list.Get(0).Message().Interface().(*anypb.Any)
	list.Append(protoreflect.ValueOfMessage(proto.Clone(original).(*anypb.Any).ProtoReflect()))
	return nil
}

func clearNextPageToken(response proto.Message) error {
	message := response.ProtoReflect()
	field := message.Descriptor().Fields().ByName("next_page_token")
	if field == nil || field.Kind() != protoreflect.StringKind {
		return fmt.Errorf("response has no next_page_token string")
	}
	message.Clear(field)
	return nil
}

func appendUnknownProtoField(response proto.Message) error {
	message := response.ProtoReflect()
	unknown := append([]byte(nil), message.GetUnknown()...)
	unknown = protowire.AppendTag(unknown, 19000, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	message.SetUnknown(unknown)
	return nil
}

func duplicateFirstListItem(response proto.Message) error {
	list, err := firstMessageList(response.ProtoReflect())
	if err != nil {
		return err
	}
	if list.Len() == 0 {
		return fmt.Errorf("response has no list item to duplicate")
	}
	item := list.Get(0).Message().Interface()
	list.Append(protoreflect.ValueOfMessage(proto.Clone(item).ProtoReflect()))
	return nil
}

func reverseFirstList(response proto.Message) error {
	list, err := firstMessageList(response.ProtoReflect())
	if err != nil {
		return err
	}
	for left, right := 0, list.Len()-1; left < right; left, right = left+1, right-1 {
		leftValue := list.Get(left)
		list.Set(left, list.Get(right))
		list.Set(right, leftValue)
	}
	return nil
}

func clearFirstListItem(response proto.Message) error {
	list, err := firstMessageList(response.ProtoReflect())
	if err != nil {
		return err
	}
	if list.Len() == 0 {
		return fmt.Errorf("response has no list item to clear")
	}
	item := list.Get(0).Message()
	item.Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		item.Clear(field)
		return true
	})
	item.SetUnknown(nil)
	return nil
}

func firstMessageList(message protoreflect.Message) (protoreflect.List, error) {
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsList() && field.Kind() == protoreflect.MessageKind &&
			field.Message().FullName() != "google.protobuf.Any" {
			return message.Mutable(field).List(), nil
		}
	}
	return nil, fmt.Errorf("response contains no repeated message field")
}

func firstAnnotationList(message protoreflect.Message) (protoreflect.List, error) {
	field := message.Descriptor().Fields().ByName("annotations")
	if field != nil && field.IsList() && field.Message() != nil &&
		field.Message().FullName() == "google.protobuf.Any" {
		return message.Mutable(field).List(), nil
	}

	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field = fields.Get(i)
		if field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			continue
		}
		if field.IsList() {
			list := message.Get(field).List()
			for j := 0; j < list.Len(); j++ {
				found, err := firstAnnotationList(list.Get(j).Message())
				if err == nil {
					return found, nil
				}
			}
			continue
		}
		found, err := firstAnnotationList(message.Get(field).Message())
		if err == nil {
			return found, nil
		}
	}
	return nil, fmt.Errorf("response contains no annotation field")
}
