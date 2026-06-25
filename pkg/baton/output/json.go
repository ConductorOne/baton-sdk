package output

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type jsonManager struct{}

const anyMessageName protoreflect.FullName = "google.protobuf.Any"

func (j *jsonManager) Output(ctx context.Context, out interface{}) error {
	var outBytes []byte
	var err error

	if m, ok := out.(proto.Message); ok {
		outBytes, err = protojson.Marshal(dropUnresolvableAnyMessages(m))
	} else {
		outBytes, err = json.Marshal(out)
	}
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(os.Stdout, string(outBytes))
	if err != nil {
		return err
	}

	return nil
}

func dropUnresolvableAnyMessages(m proto.Message) proto.Message {
	if m == nil {
		return nil
	}

	cloned := proto.Clone(m)
	pruneUnresolvableAny(cloned.ProtoReflect())
	return cloned
}

func pruneUnresolvableAny(m protoreflect.Message) bool {
	if !m.IsValid() {
		return true
	}
	if m.Descriptor().FullName() == anyMessageName {
		typeURLField := m.Descriptor().Fields().ByNumber(1)
		if typeURLField == nil {
			return false
		}
		typeURL := m.Get(typeURLField).String()
		_, err := protoregistry.GlobalTypes.FindMessageByURL(typeURL)
		return err == nil
	}

	m.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case fd.IsList():
			if messageField(fd) {
				pruneUnresolvableAnyList(value.List())
			}
		case fd.IsMap():
			if messageField(fd.MapValue()) {
				pruneUnresolvableAnyMap(value.Map())
			}
		case messageField(fd):
			if !pruneUnresolvableAny(value.Message()) {
				m.Clear(fd)
			}
		}
		return true
	})
	return true
}

func pruneUnresolvableAnyList(list protoreflect.List) {
	writeIdx := 0
	for readIdx := 0; readIdx < list.Len(); readIdx++ {
		value := list.Get(readIdx)
		if !pruneUnresolvableAny(value.Message()) {
			continue
		}
		if writeIdx != readIdx {
			list.Set(writeIdx, value)
		}
		writeIdx++
	}
	list.Truncate(writeIdx)
}

func pruneUnresolvableAnyMap(m protoreflect.Map) {
	var toDelete []protoreflect.MapKey
	m.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		if !pruneUnresolvableAny(value.Message()) {
			toDelete = append(toDelete, key)
		}
		return true
	})
	for _, key := range toDelete {
		m.Clear(key)
	}
}

func messageField(fd protoreflect.FieldDescriptor) bool {
	return fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind
}
