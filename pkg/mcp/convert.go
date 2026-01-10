package mcp

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	marshaler = protojson.MarshalOptions{
		EmitUnpopulated: false,
		UseProtoNames:   true,
	}

	unmarshaler = protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
)

// protoToMap converts a proto message to a map[string]any.
func protoToMap(msg proto.Message) (map[string]any, error) {
	if msg == nil {
		return nil, nil
	}
	jsonBytes, err := marshaler.Marshal(msg)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// protoToJSON converts a proto message to a JSON string.
func protoToJSON(msg proto.Message) (string, error) {
	if msg == nil {
		return "{}", nil
	}
	jsonBytes, err := marshaler.Marshal(msg)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// protoListToMaps converts a slice of proto messages to a slice of maps.
func protoListToMaps[T proto.Message](list []T) ([]map[string]any, error) {
	result := make([]map[string]any, 0, len(list))
	for _, item := range list {
		m, err := protoToMap(item)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, nil
}

// jsonToProto unmarshals a JSON string into a proto message.
func jsonToProto(jsonStr string, msg proto.Message) error {
	return unmarshaler.Unmarshal([]byte(jsonStr), msg)
}

// mapToProto converts a map to a proto message by going through JSON.
func mapToProto(m map[string]any, msg proto.Message) error {
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return unmarshaler.Unmarshal(jsonBytes, msg)
}
