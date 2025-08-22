package pebble

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestValueCodec_EncodeValue(t *testing.T) {
	vc := NewValueCodec()

	// Create a test resource type
	rt := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Test Resource Type",
	}

	discoveredAt := time.Now()

	// Encode without content type
	encoded, err := vc.EncodeValue(rt, discoveredAt, "")
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	// Decode and verify
	var decodedRT v2.ResourceType
	envelope, err := vc.DecodeValue(encoded, &decodedRT)
	require.NoError(t, err)

	assert.Equal(t, discoveredAt.UnixNano(), envelope.DiscoveredAt)
	assert.Empty(t, envelope.ContentType) // Should be empty for non-assets
	assert.Equal(t, rt.Id, decodedRT.Id)
	assert.Equal(t, rt.DisplayName, decodedRT.DisplayName)
}

func TestValueCodec_EncodeAsset(t *testing.T) {
	vc := NewValueCodec()

	assetData := []byte("test asset content")
	discoveredAt := time.Now()
	contentType := "text/plain"

	// Encode asset
	encoded, err := vc.EncodeAsset(assetData, discoveredAt, contentType)
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	// Decode and verify
	envelope, data, err := vc.DecodeAsset(encoded)
	require.NoError(t, err)

	assert.Equal(t, discoveredAt.UnixNano(), envelope.DiscoveredAt)
	assert.Equal(t, contentType, envelope.ContentType)
	assert.Equal(t, assetData, data)
}

func TestValueCodec_DecodeEnvelope(t *testing.T) {
	vc := NewValueCodec()

	rt := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Test Resource Type",
	}

	discoveredAt := time.Now()

	encoded, err := vc.EncodeValue(rt, discoveredAt, "")
	require.NoError(t, err)

	// Decode just the envelope
	envelope, err := vc.DecodeEnvelope(encoded)
	require.NoError(t, err)

	assert.Equal(t, discoveredAt.UnixNano(), envelope.DiscoveredAt)
	assert.Empty(t, envelope.ContentType)
	assert.NotEmpty(t, envelope.Data)
}

func TestValueCodec_GetDiscoveredAt(t *testing.T) {
	vc := NewValueCodec()

	rt := &v2.ResourceType{Id: "rt1"}
	discoveredAt := time.Now().Truncate(time.Second) // Truncate to avoid precision issues

	encoded, err := vc.EncodeValue(rt, discoveredAt, "")
	require.NoError(t, err)

	// Extract discovered_at
	extractedTime, err := vc.GetDiscoveredAt(encoded)
	require.NoError(t, err)

	assert.Equal(t, discoveredAt.UnixNano(), extractedTime.UnixNano())
}

func TestValueCodec_GetContentType(t *testing.T) {
	vc := NewValueCodec()

	tests := []struct {
		name        string
		contentType string
	}{
		{
			name:        "with content type",
			contentType: "application/json",
		},
		{
			name:        "empty content type",
			contentType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assetData := []byte("test data")
			discoveredAt := time.Now()

			encoded, err := vc.EncodeAsset(assetData, discoveredAt, tt.contentType)
			require.NoError(t, err)

			extractedContentType, err := vc.GetContentType(encoded)
			require.NoError(t, err)

			assert.Equal(t, tt.contentType, extractedContentType)
		})
	}
}

func TestValueCodec_IsNewer(t *testing.T) {
	vc := NewValueCodec()

	rt := &v2.ResourceType{Id: "rt1"}
	baseTime := time.Now()

	tests := []struct {
		name        string
		encodedTime time.Time
		compareTime time.Time
		expectNewer bool
	}{
		{
			name:        "encoded is newer",
			encodedTime: baseTime.Add(time.Hour),
			compareTime: baseTime,
			expectNewer: true,
		},
		{
			name:        "encoded is older",
			encodedTime: baseTime,
			compareTime: baseTime.Add(time.Hour),
			expectNewer: false,
		},
		{
			name:        "same time",
			encodedTime: baseTime,
			compareTime: baseTime,
			expectNewer: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := vc.EncodeValue(rt, tt.encodedTime, "")
			require.NoError(t, err)

			isNewer, err := vc.IsNewer(encoded, tt.compareTime)
			require.NoError(t, err)

			assert.Equal(t, tt.expectNewer, isNewer)
		})
	}
}

func TestValueCodec_ValidateEnvelope(t *testing.T) {
	vc := NewValueCodec()

	tests := []struct {
		name    string
		setup   func() []byte
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid envelope",
			setup: func() []byte {
				rt := &v2.ResourceType{Id: "rt1"}
				encoded, _ := vc.EncodeValue(rt, time.Now(), "")
				return encoded
			},
			wantErr: false,
		},
		{
			name: "invalid protobuf data",
			setup: func() []byte {
				return []byte("invalid protobuf data")
			},
			wantErr: true,
			errMsg:  "invalid envelope structure",
		},
		{
			name: "zero discovered_at",
			setup: func() []byte {
				vc := NewValueCodec()
				envelope := &ValueEnvelope{
					DiscoveredAt: 0,
					Data:         []byte("test"),
				}
				data, _ := vc.MarshalEnvelope(envelope)
				return data
			},
			wantErr: true,
			errMsg:  "invalid discovered_at timestamp",
		},
		{
			name: "empty data",
			setup: func() []byte {
				vc := NewValueCodec()
				envelope := &ValueEnvelope{
					DiscoveredAt: time.Now().Unix(),
					Data:         []byte{}, // Empty data should be valid for assets
				}
				data, _ := vc.MarshalEnvelope(envelope)
				return data
			},
			wantErr: false, // Empty data is now valid
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.setup()
			err := vc.ValidateEnvelope(data)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValueCodec_GetEnvelopeSize(t *testing.T) {
	vc := NewValueCodec()

	tests := []struct {
		name        string
		contentType string
	}{
		{
			name:        "no content type",
			contentType: "",
		},
		{
			name:        "with content type",
			contentType: "application/json",
		},
		{
			name:        "long content type",
			contentType: "application/vnd.api+json; charset=utf-8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := vc.GetEnvelopeSize(tt.contentType)
			assert.Greater(t, size, 0)

			// Size should be larger when content type is present
			if tt.contentType != "" {
				emptySize := vc.GetEnvelopeSize("")
				assert.Greater(t, size, emptySize)
			}
		})
	}
}

func TestValueCodec_CompareDiscoveredAt(t *testing.T) {
	vc := NewValueCodec()

	rt := &v2.ResourceType{Id: "rt1"}
	baseTime := time.Now()
	earlierTime := baseTime.Add(-time.Hour)
	laterTime := baseTime.Add(time.Hour)

	encoded1, err := vc.EncodeValue(rt, earlierTime, "")
	require.NoError(t, err)

	encoded2, err := vc.EncodeValue(rt, baseTime, "")
	require.NoError(t, err)

	encoded3, err := vc.EncodeValue(rt, laterTime, "")
	require.NoError(t, err)

	tests := []struct {
		name     string
		data1    []byte
		data2    []byte
		expected int
	}{
		{
			name:     "first is earlier",
			data1:    encoded1,
			data2:    encoded2,
			expected: -1,
		},
		{
			name:     "first is later",
			data1:    encoded3,
			data2:    encoded2,
			expected: 1,
		},
		{
			name:     "same time",
			data1:    encoded2,
			data2:    encoded2,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := vc.CompareDiscoveredAt(tt.data1, tt.data2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueCodec_CreateEnvelopeWithCurrentTime(t *testing.T) {
	vc := NewValueCodec()

	data := []byte("test data")
	contentType := "text/plain"

	before := time.Now()
	encoded, err := vc.CreateEnvelopeWithCurrentTime(data, contentType)
	after := time.Now()

	require.NoError(t, err)

	// Decode and verify
	envelope, err := vc.DecodeEnvelope(encoded)
	require.NoError(t, err)

	discoveredAt := time.Unix(0, envelope.DiscoveredAt)
	// Allow for some time drift due to nanosecond timestamp precision
	assert.True(t, discoveredAt.UnixNano() >= before.UnixNano() && discoveredAt.UnixNano() <= after.UnixNano())
	assert.Equal(t, contentType, envelope.ContentType)
	assert.Equal(t, data, envelope.Data)
}

func TestValueCodec_ExtractInnerData(t *testing.T) {
	vc := NewValueCodec()

	rt := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Test Resource Type",
	}

	// Encode the resource type
	encoded, err := vc.EncodeValue(rt, time.Now(), "")
	require.NoError(t, err)

	// Extract inner data
	innerData, err := vc.ExtractInnerData(encoded)
	require.NoError(t, err)

	// Verify we can unmarshal the inner data directly
	var decodedRT v2.ResourceType
	err = proto.Unmarshal(innerData, &decodedRT)
	require.NoError(t, err)

	assert.Equal(t, rt.Id, decodedRT.Id)
	assert.Equal(t, rt.DisplayName, decodedRT.DisplayName)
}

func TestValueCodec_UpdateDiscoveredAt(t *testing.T) {
	vc := NewValueCodec()

	rt := &v2.ResourceType{Id: "rt1"}
	originalTime := time.Now().Add(-time.Hour)
	newTime := time.Now()

	// Encode with original time
	encoded, err := vc.EncodeValue(rt, originalTime, "")
	require.NoError(t, err)

	// Update discovered_at
	updated, err := vc.UpdateDiscoveredAt(encoded, newTime)
	require.NoError(t, err)

	// Verify the timestamp was updated
	extractedTime, err := vc.GetDiscoveredAt(updated)
	require.NoError(t, err)
	assert.Equal(t, newTime.Unix(), extractedTime.Unix())

	// Verify the inner data is unchanged
	var decodedRT v2.ResourceType
	_, err = vc.DecodeValue(updated, &decodedRT)
	require.NoError(t, err)
	assert.Equal(t, rt.Id, decodedRT.Id)
}

func TestValueCodec_CloneEnvelope(t *testing.T) {
	vc := NewValueCodec()

	rt := &v2.ResourceType{Id: "rt1"}
	discoveredAt := time.Now()
	contentType := "application/json"

	// Encode original
	original, err := vc.EncodeValue(rt, discoveredAt, contentType)
	require.NoError(t, err)

	// Create new inner data
	newRT := &v2.ResourceType{Id: "rt2", DisplayName: "New RT"}
	newInnerData, err := proto.Marshal(newRT)
	require.NoError(t, err)

	// Clone with new inner data
	cloned, err := vc.CloneEnvelope(original, newInnerData)
	require.NoError(t, err)

	// Verify the envelope metadata is preserved
	clonedEnvelope, err := vc.DecodeEnvelope(cloned)
	require.NoError(t, err)
	assert.Equal(t, discoveredAt.UnixNano(), clonedEnvelope.DiscoveredAt)
	assert.Equal(t, contentType, clonedEnvelope.ContentType)

	// Verify the inner data is updated
	var decodedRT v2.ResourceType
	err = proto.Unmarshal(clonedEnvelope.Data, &decodedRT)
	require.NoError(t, err)
	assert.Equal(t, newRT.Id, decodedRT.Id)
	assert.Equal(t, newRT.DisplayName, decodedRT.DisplayName)
}

func TestValueCodec_RoundTripWithDifferentTypes(t *testing.T) {
	vc := NewValueCodec()
	discoveredAt := time.Now()

	tests := []struct {
		name        string
		message     proto.Message
		contentType string
	}{
		{
			name: "resource type",
			message: &v2.ResourceType{
				Id:          "rt1",
				DisplayName: "Test Resource Type",
			},
			contentType: "",
		},
		{
			name: "resource",
			message: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "rt1",
					Resource:     "res1",
				},
				DisplayName: "Test Resource",
			},
			contentType: "",
		},
		{
			name: "entitlement",
			message: &v2.Entitlement{
				Id:          "ent1",
				DisplayName: "Test Entitlement",
			},
			contentType: "",
		},
		{
			name: "grant",
			message: &v2.Grant{
				Id: "grant1",
				Principal: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "user",
						Resource:     "user1",
					},
				},
			},
			contentType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := vc.EncodeValue(tt.message, discoveredAt, tt.contentType)
			require.NoError(t, err)

			// Create a new instance of the same type for decoding
			decoded := proto.Clone(tt.message)
			proto.Reset(decoded)

			// Decode
			envelope, err := vc.DecodeValue(encoded, decoded)
			require.NoError(t, err)

			// Verify envelope
			assert.Equal(t, discoveredAt.UnixNano(), envelope.DiscoveredAt)
			assert.Equal(t, tt.contentType, envelope.ContentType)

			// Verify the decoded message matches the original
			assert.True(t, proto.Equal(tt.message, decoded))
		})
	}
}

func TestValueCodec_LargeData(t *testing.T) {
	vc := NewValueCodec()

	// Create a large asset
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	discoveredAt := time.Now()
	contentType := "application/octet-stream"

	// Encode
	encoded, err := vc.EncodeAsset(largeData, discoveredAt, contentType)
	require.NoError(t, err)

	// Decode
	envelope, data, err := vc.DecodeAsset(encoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, discoveredAt.UnixNano(), envelope.DiscoveredAt)
	assert.Equal(t, contentType, envelope.ContentType)
	assert.Equal(t, largeData, data)
}

func TestValueCodec_ErrorCases(t *testing.T) {
	vc := NewValueCodec()

	t.Run("decode invalid data", func(t *testing.T) {
		invalidData := []byte("not protobuf data")

		_, err := vc.DecodeEnvelope(invalidData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal envelope")
	})

	t.Run("decode value with wrong message type", func(t *testing.T) {
		rt := &v2.ResourceType{Id: "rt1"}
		encoded, err := vc.EncodeValue(rt, time.Now(), "")
		require.NoError(t, err)

		// Try to decode as a different type
		var resource v2.Resource
		_, err = vc.DecodeValue(encoded, &resource)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal inner message")
	})

	t.Run("get discovered_at from invalid data", func(t *testing.T) {
		invalidData := []byte("not protobuf data")

		_, err := vc.GetDiscoveredAt(invalidData)
		assert.Error(t, err)
	})

	t.Run("compare discovered_at with invalid data", func(t *testing.T) {
		rt := &v2.ResourceType{Id: "rt1"}
		validData, err := vc.EncodeValue(rt, time.Now(), "")
		require.NoError(t, err)

		invalidData := []byte("not protobuf data")

		_, err = vc.CompareDiscoveredAt(validData, invalidData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get discovered_at from second value")

		_, err = vc.CompareDiscoveredAt(invalidData, validData)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get discovered_at from first value")
	})
}

func TestValueEnvelope_Methods(t *testing.T) {
	envelope := &ValueEnvelope{
		DiscoveredAt: time.Now().Unix(),
		ContentType:  "text/plain",
		Data:         []byte("test data"),
	}

	// Test String method
	str := envelope.String()
	assert.Contains(t, str, "ValueEnvelope")
	assert.Contains(t, str, "text/plain")
	assert.Contains(t, str, "DataLen: 9")

	// Test Reset method
	envelope.Reset()
	assert.Equal(t, int64(0), envelope.DiscoveredAt)
	assert.Empty(t, envelope.ContentType)
	assert.Empty(t, envelope.Data)

	// ValueEnvelope no longer implements ProtoMessage since we use binary encoding
}

// Benchmark tests
func BenchmarkValueCodec_EncodeValue(b *testing.B) {
	vc := NewValueCodec()
	rt := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Test Resource Type",
	}
	discoveredAt := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vc.EncodeValue(rt, discoveredAt, "")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueCodec_DecodeValue(b *testing.B) {
	vc := NewValueCodec()
	rt := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Test Resource Type",
	}

	encoded, err := vc.EncodeValue(rt, time.Now(), "")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded v2.ResourceType
		_, err := vc.DecodeValue(encoded, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueCodec_GetDiscoveredAt(b *testing.B) {
	vc := NewValueCodec()
	rt := &v2.ResourceType{Id: "rt1"}

	encoded, err := vc.EncodeValue(rt, time.Now(), "")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := vc.GetDiscoveredAt(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
