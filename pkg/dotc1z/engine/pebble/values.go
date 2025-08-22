package pebble

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

// ValueCodec handles value serialization with minimal metadata envelope.
type ValueCodec struct{}

// NewValueCodec creates a new ValueCodec instance.
func NewValueCodec() *ValueCodec {
	return &ValueCodec{}
}

// ValueEnvelope is a simple struct for encoding metadata with values.
type ValueEnvelope struct {
	DiscoveredAt int64
	ContentType  string
	Data         []byte
}

// Reset resets the ValueEnvelope to its zero state.
func (ve *ValueEnvelope) Reset() {
	*ve = ValueEnvelope{}
}

// String returns a string representation of the ValueEnvelope.
func (ve *ValueEnvelope) String() string {
	return fmt.Sprintf("ValueEnvelope{DiscoveredAt: %d, ContentType: %q, DataLen: %d}",
		ve.DiscoveredAt, ve.ContentType, len(ve.Data))
}

// MarshalEnvelope encodes a ValueEnvelope using binary encoding.
// Format: [discovered_at:8][content_type_len:4][content_type:N][data_len:4][data:N]
func (vc *ValueCodec) MarshalEnvelope(envelope *ValueEnvelope) ([]byte, error) {
	var buf bytes.Buffer

	// Write discovered_at (8 bytes)
	err := binary.Write(&buf, binary.BigEndian, envelope.DiscoveredAt)
	if err != nil {
		return nil, fmt.Errorf("failed to write discovered_at: %w", err)
	}

	// Write content_type length and content_type
	contentTypeBytes := []byte(envelope.ContentType)
	err = binary.Write(&buf, binary.BigEndian, uint32(len(contentTypeBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed to write content_type length: %w", err)
	}
	buf.Write(contentTypeBytes)

	// Write data length and data
	err = binary.Write(&buf, binary.BigEndian, uint32(len(envelope.Data)))
	if err != nil {
		return nil, fmt.Errorf("failed to write data length: %w", err)
	}
	buf.Write(envelope.Data)

	return buf.Bytes(), nil
}

// unmarshalEnvelope decodes a ValueEnvelope from binary encoding.
func (vc *ValueCodec) unmarshalEnvelope(data []byte) (*ValueEnvelope, error) {
	if len(data) < 16 { // minimum: 8 + 4 + 4 = 16 bytes
		return nil, fmt.Errorf("envelope data too short: %d bytes", len(data))
	}

	buf := bytes.NewReader(data)
	envelope := &ValueEnvelope{}

	// Read discovered_at
	err := binary.Read(buf, binary.BigEndian, &envelope.DiscoveredAt)
	if err != nil {
		return nil, fmt.Errorf("failed to read discovered_at: %w", err)
	}

	// Read content_type length and content_type
	var contentTypeLen uint32
	err = binary.Read(buf, binary.BigEndian, &contentTypeLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read content_type length: %w", err)
	}

	if contentTypeLen > 0 {
		contentTypeBytes := make([]byte, contentTypeLen)
		_, err = buf.Read(contentTypeBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read content_type: %w", err)
		}
		envelope.ContentType = string(contentTypeBytes)
	}

	// Read data length and data
	var dataLen uint32
	err = binary.Read(buf, binary.BigEndian, &dataLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read data length: %w", err)
	}

	if dataLen > 0 {
		envelope.Data = make([]byte, dataLen)
		_, err = buf.Read(envelope.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
	} else {
		envelope.Data = []byte{}
	}

	return envelope, nil
}

// EncodeValue encodes a protobuf message with metadata envelope.
func (vc *ValueCodec) EncodeValue(msg proto.Message, discoveredAt time.Time, contentType string) ([]byte, error) {
	// Serialize the inner message
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inner message: %w", err)
	}

	// Create the envelope
	envelope := &ValueEnvelope{
		DiscoveredAt: discoveredAt.UnixNano(),
		ContentType:  contentType,
		Data:         data,
	}

	// Serialize the envelope
	envelopeData, err := vc.MarshalEnvelope(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope: %w", err)
	}

	return envelopeData, nil
}

// EncodeAsset encodes raw asset data with metadata envelope.
func (vc *ValueCodec) EncodeAsset(data []byte, discoveredAt time.Time, contentType string) ([]byte, error) {
	// Create the envelope with raw data
	envelope := &ValueEnvelope{
		DiscoveredAt: discoveredAt.UnixNano(),
		ContentType:  contentType,
		Data:         data,
	}

	// Serialize the envelope
	envelopeData, err := vc.MarshalEnvelope(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal asset envelope: %w", err)
	}

	return envelopeData, nil
}

// DecodeValue decodes a value envelope and unmarshals the inner message.
func (vc *ValueCodec) DecodeValue(data []byte, msg proto.Message) (*ValueEnvelope, error) {
	// Unmarshal the envelope
	envelope, err := vc.unmarshalEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	// Unmarshal the inner message
	err = proto.Unmarshal(envelope.Data, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal inner message: %w", err)
	}

	return envelope, nil
}

// DecodeAsset decodes an asset envelope and returns the raw data.
func (vc *ValueCodec) DecodeAsset(data []byte) (*ValueEnvelope, []byte, error) {
	// Unmarshal the envelope
	envelope, err := vc.unmarshalEnvelope(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal asset envelope: %w", err)
	}

	return envelope, envelope.Data, nil
}

// DecodeEnvelope decodes just the envelope without unmarshaling the inner data.
func (vc *ValueCodec) DecodeEnvelope(data []byte) (*ValueEnvelope, error) {
	envelope, err := vc.unmarshalEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	return envelope, nil
}

// GetDiscoveredAt extracts the discovered_at timestamp from encoded data.
func (vc *ValueCodec) GetDiscoveredAt(data []byte) (time.Time, error) {
	envelope, err := vc.DecodeEnvelope(data)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, envelope.DiscoveredAt), nil
}

// GetContentType extracts the content type from encoded data.
func (vc *ValueCodec) GetContentType(data []byte) (string, error) {
	envelope, err := vc.DecodeEnvelope(data)
	if err != nil {
		return "", err
	}

	return envelope.ContentType, nil
}

// IsNewer compares the discovered_at timestamp in the encoded data with the given time.
func (vc *ValueCodec) IsNewer(encodedData []byte, compareTime time.Time) (bool, error) {
	discoveredAt, err := vc.GetDiscoveredAt(encodedData)
	if err != nil {
		return false, err
	}

	return discoveredAt.After(compareTime), nil
}

// ValidateEnvelope validates that the envelope structure is correct.
func (vc *ValueCodec) ValidateEnvelope(data []byte) error {
	envelope, err := vc.DecodeEnvelope(data)
	if err != nil {
		return fmt.Errorf("invalid envelope structure: %w", err)
	}

	if envelope.DiscoveredAt <= 0 {
		return fmt.Errorf("invalid discovered_at timestamp: %d", envelope.DiscoveredAt)
	}

	// Note: Empty data is allowed for assets (e.g., empty files)
	// No need to validate data content since both empty and non-empty data are valid

	return nil
}

// GetEnvelopeSize returns the size of the envelope overhead.
func (vc *ValueCodec) GetEnvelopeSize(contentType string) int {
	// 8 bytes for discovered_at + 4 bytes for content_type length + content_type + 4 bytes for data length
	return 8 + 4 + len(contentType) + 4
}

// CompareDiscoveredAt compares the discovered_at timestamps of two encoded values.
func (vc *ValueCodec) CompareDiscoveredAt(data1, data2 []byte) (int, error) {
	time1, err := vc.GetDiscoveredAt(data1)
	if err != nil {
		return 0, fmt.Errorf("failed to get discovered_at from first value: %w", err)
	}

	time2, err := vc.GetDiscoveredAt(data2)
	if err != nil {
		return 0, fmt.Errorf("failed to get discovered_at from second value: %w", err)
	}

	if time1.Before(time2) {
		return -1, nil
	} else if time1.After(time2) {
		return 1, nil
	}
	return 0, nil
}

// CreateEnvelopeWithCurrentTime creates an envelope with the current timestamp.
func (vc *ValueCodec) CreateEnvelopeWithCurrentTime(data []byte, contentType string) ([]byte, error) {
	envelope := &ValueEnvelope{
		DiscoveredAt: time.Now().UnixNano(),
		ContentType:  contentType,
		Data:         data,
	}

	envelopeData, err := vc.MarshalEnvelope(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope: %w", err)
	}

	return envelopeData, nil
}

// ExtractInnerData extracts just the inner data from an encoded value.
func (vc *ValueCodec) ExtractInnerData(encodedData []byte) ([]byte, error) {
	envelope, err := vc.DecodeEnvelope(encodedData)
	if err != nil {
		return nil, err
	}

	return envelope.Data, nil
}

// UpdateDiscoveredAt updates the discovered_at timestamp in an encoded value.
func (vc *ValueCodec) UpdateDiscoveredAt(encodedData []byte, newTime time.Time) ([]byte, error) {
	envelope, err := vc.DecodeEnvelope(encodedData)
	if err != nil {
		return nil, err
	}

	envelope.DiscoveredAt = newTime.UnixNano()

	updatedData, err := vc.MarshalEnvelope(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated envelope: %w", err)
	}

	return updatedData, nil
}

// CloneEnvelope creates a copy of an encoded value with new inner data.
func (vc *ValueCodec) CloneEnvelope(encodedData []byte, newInnerData []byte) ([]byte, error) {
	envelope, err := vc.DecodeEnvelope(encodedData)
	if err != nil {
		return nil, err
	}

	newEnvelope := &ValueEnvelope{
		DiscoveredAt: envelope.DiscoveredAt,
		ContentType:  envelope.ContentType,
		Data:         newInnerData,
	}

	clonedData, err := vc.MarshalEnvelope(newEnvelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cloned envelope: %w", err)
	}

	return clonedData, nil
}
