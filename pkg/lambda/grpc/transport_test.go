package grpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	// Import packages to register protobuf types.
	_ "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	_ "github.com/conductorone/baton-sdk/pb/c1/transport/v1"

	batonv1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

func TestRequest_UnmarshalJSON_WithUnknownAnnotations(t *testing.T) {
	tests := []struct {
		name                     string
		jsonInput                string
		expectError              bool
		expectedAnnotationsCount int
	}{
		{
			name: "should filter out unknown annotation types",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host",
					"annotations": [
						{
							"@type": "type.googleapis.com/c1.connector.v2.GroupTrait",
							"profile": {}
						},
						{
							"@type": "type.googleapis.com/c1.connector.v2.UnknownType",
							"someField": "someValue"
						},
						{
							"@type": "type.googleapis.com/c1.connector.v2.RoleTrait",
							"profile": {}
						}
					]
				},
				"headers": {}
			}`,
			expectError:              false,
			expectedAnnotationsCount: 2, // Only GroupTrait and RoleTrait should remain
		},
		{
			name: "should handle empty annotations array",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host",
					"annotations": []
				},
				"headers": {}
			}`,
			expectError:              false,
			expectedAnnotationsCount: 0,
		},
		{
			name: "should handle missing annotations field",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host"
				},
				"headers": {}
			}`,
			expectError:              false,
			expectedAnnotationsCount: 0,
		},
		{
			name: "should handle all unknown annotations",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host",
					"annotations": [
						{
							"@type": "type.googleapis.com/c1.connector.v2.UnknownType1",
							"field1": "value1"
						},
						{
							"@type": "type.googleapis.com/c1.connector.v2.UnknownType2",
							"field2": "value2"
						}
					]
				},
				"headers": {}
			}`,
			expectError:              false,
			expectedAnnotationsCount: 0,
		},
		{
			name: "should handle malformed annotation objects",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host",
					"annotations": [
						{
							"@type": "type.googleapis.com/c1.connector.v2.GroupTrait",
							"profile": {}
						},
						"not-an-object",
						{
							"@type": "type.googleapis.com/c1.connector.v2.RoleTrait",
							"profile": {}
						},
						{
							"no-type-field": "value"
						}
					]
				},
				"headers": {}
			}`,
			expectError:              false,
			expectedAnnotationsCount: 2,
		},
		{
			name: "should handle invalid JSON",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host",
					"annotations": [
						{
							"@type": "type.googleapis.com/c1.connector.v2.GroupTrait",
							"profile": {}
						}
					]
				},
				"headers": {}
			`, // Missing closing brace.
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &Request{}
			err := req.UnmarshalJSON([]byte(tt.jsonInput))

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, req.msg)

			// Verify the method is preserved.
			assert.Equal(t, "/c1.connectorapi.baton.v1.BatonService/Hello", req.Method())

			// Verify the request was unmarshaled.
			assert.NotNil(t, req.msg.GetReq())

			// If we expect annotations, verify they were filtered correctly.
			if tt.expectedAnnotationsCount >= 0 {
				// Unmarshal the actual request message to check annotations
				reqAny := req.msg.GetReq()
				require.NotNil(t, reqAny, "Request should have a req field")

				// Unmarshal the anypb.Any to get the actual request message
				var actualReq batonv1.BatonServiceHelloRequest
				err = anypb.UnmarshalTo(reqAny, &actualReq, proto.UnmarshalOptions{})
				require.NoError(t, err, "Should be able to unmarshal the request")

				// Check the actual annotations in the unmarshaled protobuf message
				actualAnnotations := actualReq.GetAnnotations()
				actualCount := len(actualAnnotations)
				assert.Equal(t, tt.expectedAnnotationsCount, actualCount,
					"Expected %d annotations after filtering, got %d", tt.expectedAnnotationsCount, actualCount)
			}
		})
	}
}

func TestRequest_UnmarshalJSON_WithKnownTypes(t *testing.T) {
	// Test with a request that contains only known types.
	jsonInput := `{
		"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
		"req": {
			"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
			"host_id": "test-host",
			"annotations": [
				{
					"@type": "type.googleapis.com/c1.connector.v2.GroupTrait",
					"profile": {
						"fields": {
							"name": {
								"stringValue": "test-group"
							}
						}
					}
				},
				{
					"@type": "type.googleapis.com/c1.connector.v2.RoleTrait",
					"profile": {
						"fields": {
							"name": {
								"stringValue": "test-role"
							}
						}
					}
				}
			]
		},
		"headers": {
			"fields": {
				"content-type": {
					"listValue": {
						"values": [
							{
								"stringValue": "application/grpc+proto"
							}
						]
					}
				}
			}
		}
	}`

	req := &Request{}
	err := req.UnmarshalJSON([]byte(jsonInput))

	require.NoError(t, err)
	assert.NotNil(t, req.msg)
	assert.Equal(t, "/c1.connectorapi.baton.v1.BatonService/Hello", req.Method())
	assert.NotNil(t, req.msg.GetReq())
	assert.NotNil(t, req.msg.GetHeaders())
}

func TestRequest_UnmarshalJSON_RegressionTest(t *testing.T) {
	// Test that the original functionality still works for requests without annotations.
	jsonInput := `{
		"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
		"req": {
			"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
			"host_id": "test-host"
		},
		"headers": {
			"fields": {
				"content-type": {
					"listValue": {
						"values": [
							{
								"stringValue": "application/grpc+proto"
							}
						]
					}
				}
			}
		}
	}`

	req := &Request{}
	err := req.UnmarshalJSON([]byte(jsonInput))

	require.NoError(t, err)
	assert.NotNil(t, req.msg)
	assert.Equal(t, "/c1.connectorapi.baton.v1.BatonService/Hello", req.Method())
	assert.NotNil(t, req.msg.GetReq())
	assert.NotNil(t, req.msg.GetHeaders())
}

// BenchmarkRequest_UnmarshalJSON benchmarks the unmarshaling performance.
func BenchmarkRequest_UnmarshalJSON(b *testing.B) {
	jsonInput := `{
		"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
		"req": {
			"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
			"host_id": "test-host",
			"annotations": [
				{
					"@type": "type.googleapis.com/c1.connector.v2.GroupTrait",
					"profile": {}
				},
				{
					"@type": "type.googleapis.com/c1.connector.v2.UnknownType",
					"field": "value"
				},
				{
					"@type": "type.googleapis.com/c1.connector.v2.RoleTrait",
					"profile": {}
				}
			]
		},
		"headers": {}
	}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := &Request{}
		_ = req.UnmarshalJSON([]byte(jsonInput))
	}
}

// TestRequest_UnmarshalJSON_EdgeCases tests various edge cases.
func TestRequest_UnmarshalJSON_RoundTripComparison(t *testing.T) {
	// Test that unmarshal -> marshal -> unmarshal preserves data integrity
	originalJSON := `{
		"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
		"req": {
			"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
			"host_id": "test-host-123",
			"task_id": "task-456",
			"annotations": [
				{
					"@type": "type.googleapis.com/c1.connector.v2.GroupTrait",
					"profile": {
						"fields": {
							"name": {
								"stringValue": "test-group"
							}
						}
					}
				},
				{
					"@type": "type.googleapis.com/c1.connector.v2.RoleTrait",
					"profile": {
						"fields": {
							"name": {
								"stringValue": "test-role"
							}
						}
					}
				},
				{
					"@type": "type.googleapis.com/c1.connector.v2.UnknownType",
					"field": "should-be-filtered"
				}
			]
		},
		"headers": {
			"fields": {
				"content-type": {
					"listValue": {
						"values": [
							{
								"stringValue": "application/grpc+proto"
							}
						]
					}
				},
				"user-agent": {
					"listValue": {
						"values": [
							{
								"stringValue": "baton-sdk/1.0.0"
							}
						]
					}
				}
			}
		}
	}`

	// Step 1: Unmarshal original JSON
	req1 := &Request{}
	err := req1.UnmarshalJSON([]byte(originalJSON))
	require.NoError(t, err, "First unmarshal should succeed")
	assert.NotNil(t, req1.msg)
	assert.Equal(t, "/c1.connectorapi.baton.v1.BatonService/Hello", req1.Method())

	// Step 2: Marshal back to JSON
	roundTripJSON, err := req1.MarshalJSON()
	require.NoError(t, err, "Marshal should succeed")
	assert.NotEmpty(t, roundTripJSON)

	// Step 3: Unmarshal the round-trip JSON
	req2 := &Request{}
	err = req2.UnmarshalJSON(roundTripJSON)
	require.NoError(t, err, "Second unmarshal should succeed")
	assert.NotNil(t, req2.msg)

	// Step 4: Compare the two requests
	assert.Equal(t, req1.Method(), req2.Method(), "Method should be preserved")

	// Compare the actual protobuf messages
	assert.Equal(t, req1.msg.GetMethod(), req2.msg.GetMethod(), "Method field should match")

	// Compare headers
	if req1.msg.GetHeaders() != nil {
		assert.Equal(t, req1.msg.GetHeaders().String(), req2.msg.GetHeaders().String(), "Headers should match")
	} else {
		assert.Nil(t, req2.msg.GetHeaders(), "Both headers should be nil")
	}

	// Compare the request payload (req field)
	req1Any := req1.msg.GetReq()
	req2Any := req2.msg.GetReq()

	if req1Any != nil {
		require.NotNil(t, req2Any, "Both req fields should be present")

		// Unmarshal both to the actual request type
		var req1Actual batonv1.BatonServiceHelloRequest
		err = anypb.UnmarshalTo(req1Any, &req1Actual, proto.UnmarshalOptions{})
		require.NoError(t, err, "Should unmarshal req1 to actual type")

		var req2Actual batonv1.BatonServiceHelloRequest
		err = anypb.UnmarshalTo(req2Any, &req2Actual, proto.UnmarshalOptions{})
		require.NoError(t, err, "Should unmarshal req2 to actual type")

		// Compare the actual request data
		assert.Equal(t, req1Actual.GetHostId(), req2Actual.GetHostId(), "HostId should match")
		assert.Equal(t, req1Actual.GetTaskId(), req2Actual.GetTaskId(), "TaskId should match")

		// Compare annotations - should have same count after filtering
		req1Annotations := req1Actual.GetAnnotations()
		req2Annotations := req2Actual.GetAnnotations()

		assert.Equal(t, len(req1Annotations), len(req2Annotations),
			"Annotation count should match after round-trip")

		// Verify that unknown annotations were filtered out in both cases
		// (both should only have the 2 known types: GroupTrait and RoleTrait)
		assert.Equal(t, 2, len(req1Annotations), "First unmarshal should have 2 known annotations")
		assert.Equal(t, 2, len(req2Annotations), "Second unmarshal should have 2 known annotations")

		// Verify the annotations are the same
		for i, ann1 := range req1Annotations {
			ann2 := req2Annotations[i]
			assert.Equal(t, ann1.GetTypeUrl(), ann2.GetTypeUrl(),
				"Annotation %d type URL should match", i)
			assert.Equal(t, ann1.GetValue(), ann2.GetValue(),
				"Annotation %d value should match", i)
		}
	} else {
		assert.Nil(t, req2Any, "Both req fields should be nil")
	}

	// Step 5: Verify that the round-trip JSON is valid and can be parsed
	var roundTripData map[string]interface{}
	err = json.Unmarshal(roundTripJSON, &roundTripData)
	require.NoError(t, err, "Round-trip JSON should be valid")

	// Verify key fields are present
	assert.Equal(t, "/c1.connectorapi.baton.v1.BatonService/Hello", roundTripData["method"])
	assert.NotNil(t, roundTripData["req"])
	assert.NotNil(t, roundTripData["headers"])
}

func TestRequest_UnmarshalJSON_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		jsonInput   string
		expectError bool
	}{
		{
			name: "nil annotations field",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host",
					"annotations": null
				},
				"headers": {}
			}`,
			expectError: false,
		},
		{
			name: "annotations field is not an array",
			jsonInput: `{
				"method": "/c1.connectorapi.baton.v1.BatonService/Hello",
				"req": {
					"@type": "type.googleapis.com/c1.connectorapi.baton.v1.BatonServiceHelloRequest",
					"host_id": "test-host",
					"annotations": "not-an-array"
				},
				"headers": {}
			}`,
			expectError: true, // This should be an error because the JSON is malformed.
		},
		{
			name: "empty request object",
			jsonInput: `{
				"method": "/test",
				"req": {},
				"headers": {}
			}`,
			expectError: false,
		},
		{
			name: "missing req field",
			jsonInput: `{
				"method": "/test",
				"headers": {}
			}`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &Request{}
			err := req.UnmarshalJSON([]byte(tt.jsonInput))

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, req.msg)
			}
		})
	}
}
