package dotc1z

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/glebarez/go-sqlite"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const grantSourcesFieldNumber = 5

// updateProtobufField updates a specific field in a protobuf wire-format blob without
// unmarshalling other fields. It preserves all fields except the target field.
func updateProtobufField(blob []byte, fieldNumber protowire.Number, newValue []byte) ([]byte, error) {
	if len(blob) == 0 {
		// Empty blob - append the new field if we have a value
		if len(newValue) == 0 {
			return blob, nil
		}
		result := protowire.AppendTag(nil, fieldNumber, protowire.BytesType)
		result = protowire.AppendBytes(result, newValue)
		return result, nil
	}

	var result []byte
	var fieldFound bool
	b := blob

	// Iterate through all fields in the wire format
	for len(b) > 0 {
		// Parse the tag (field number and wire type)
		num, wtyp, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return nil, fmt.Errorf("invalid protobuf tag: %w", protowire.ParseError(tagLen))
		}

		// Get the field value length
		valLen := protowire.ConsumeFieldValue(num, wtyp, b[tagLen:])
		if valLen < 0 {
			return nil, fmt.Errorf("invalid protobuf field value: %w", protowire.ParseError(valLen))
		}

		fieldEnd := tagLen + valLen

		if num == fieldNumber {
			// This is the field we want to update
			fieldFound = true
			if len(newValue) > 0 {
				// Replace with new value
				result = protowire.AppendTag(result, fieldNumber, protowire.BytesType)
				result = protowire.AppendBytes(result, newValue)
			}
			// If newValue is empty, we skip this field (effectively removing it)
		} else {
			// Copy field as-is
			result = append(result, b[0:fieldEnd]...)
		}

		b = b[fieldEnd:]
	}

	// If field wasn't found and we have a new value, append it
	if !fieldFound && len(newValue) > 0 {
		result = protowire.AppendTag(result, fieldNumber, protowire.BytesType)
		result = protowire.AppendBytes(result, newValue)
	}

	return result, nil
}

// jsonSourcesToProtobuf converts a JSON-encoded sources map to protobuf-encoded GrantSources message
func jsonSourcesToProtobuf(jsonSources []byte) ([]byte, error) {
	if len(jsonSources) == 0 || string(jsonSources) == "{}" || string(jsonSources) == "null" {
		// Empty sources - return empty GrantSources message
		return []byte{}, nil
	}

	var sourcesMap map[string]*v2.GrantSources_GrantSource
	if err := json.Unmarshal(jsonSources, &sourcesMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON sources: %w", err)
	}

	if len(sourcesMap) == 0 {
		return []byte{}, nil
	}

	grantSources := &v2.GrantSources{
		Sources: sourcesMap,
	}

	protoData, err := proto.Marshal(grantSources)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GrantSources: %w", err)
	}

	return protoData, nil
}

// updateGrantSourcesProto is the SQLite function that updates field 5 (sources) in a Grant protobuf blob
func updateGrantSourcesProto(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("update_grant_sources_proto expects 2 arguments: (blob, json_sources)")
	}

	blob, ok := args[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("first argument must be a blob")
	}

	jsonSources, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("second argument must be a string (JSON)")
	}

	// Convert JSON sources to protobuf-encoded GrantSources
	newSourcesProto, err := jsonSourcesToProtobuf([]byte(jsonSources))
	if err != nil {
		return nil, fmt.Errorf("failed to convert JSON to protobuf: %w", err)
	}

	// Update field 5 in the protobuf blob
	updatedBlob, err := updateProtobufField(blob, grantSourcesFieldNumber, newSourcesProto)
	if err != nil {
		return nil, fmt.Errorf("failed to update protobuf field: %w", err)
	}

	return updatedBlob, nil
}

func init() {
	// Register the SQLite function for updating grant sources in protobuf blobs
	err := sqlite.RegisterScalarFunction(
		"update_grant_sources_proto",
		2, // Two arguments: blob and json_sources
		updateGrantSourcesProto,
	)
	if err != nil {
		panic(fmt.Sprintf("failed to register update_grant_sources_proto function: %v", err))
	}
}
