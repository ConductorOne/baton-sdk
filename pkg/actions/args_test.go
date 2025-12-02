package actions

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGetStringArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantVal string
		wantOk  bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantVal: "",
			wantOk:  false,
		},
		{
			name:    "nil fields",
			args:    &structpb.Struct{},
			key:     "test",
			wantVal: "",
			wantOk:  false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewStringValue("value"),
				},
			},
			key:     "test",
			wantVal: "",
			wantOk:  false,
		},
		{
			name: "wrong type",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewNumberValue(123),
				},
			},
			key:     "test",
			wantVal: "",
			wantOk:  false,
		},
		{
			name: "success",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("hello"),
				},
			},
			key:     "test",
			wantVal: "hello",
			wantOk:  true,
		},
		{
			name: "empty string",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue(""),
				},
			},
			key:     "test",
			wantVal: "",
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetStringArg(tt.args, tt.key)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.wantVal, val)
		})
	}
}

func TestGetIntArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantVal int64
		wantOk  bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantVal: 0,
			wantOk:  false,
		},
		{
			name:    "nil fields",
			args:    &structpb.Struct{},
			key:     "test",
			wantVal: 0,
			wantOk:  false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewNumberValue(42),
				},
			},
			key:     "test",
			wantVal: 0,
			wantOk:  false,
		},
		{
			name: "wrong type",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("not a number"),
				},
			},
			key:     "test",
			wantVal: 0,
			wantOk:  false,
		},
		{
			name: "success",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewNumberValue(42),
				},
			},
			key:     "test",
			wantVal: 42,
			wantOk:  true,
		},
		{
			name: "negative number",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewNumberValue(-100),
				},
			},
			key:     "test",
			wantVal: -100,
			wantOk:  true,
		},
		{
			name: "zero",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewNumberValue(0),
				},
			},
			key:     "test",
			wantVal: 0,
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetIntArg(tt.args, tt.key)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.wantVal, val)
		})
	}
}

func TestGetBoolArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantVal bool
		wantOk  bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantVal: false,
			wantOk:  false,
		},
		{
			name:    "nil fields",
			args:    &structpb.Struct{},
			key:     "test",
			wantVal: false,
			wantOk:  false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewBoolValue(true),
				},
			},
			key:     "test",
			wantVal: false,
			wantOk:  false,
		},
		{
			name: "wrong type",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("true"),
				},
			},
			key:     "test",
			wantVal: false,
			wantOk:  false,
		},
		{
			name: "success true",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewBoolValue(true),
				},
			},
			key:     "test",
			wantVal: true,
			wantOk:  true,
		},
		{
			name: "success false",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewBoolValue(false),
				},
			},
			key:     "test",
			wantVal: false,
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetBoolArg(tt.args, tt.key)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.wantVal, val)
		})
	}
}

func TestGetResourceIDArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantVal *v2.ResourceId
		wantOk  bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name:    "nil fields",
			args:    &structpb.Struct{},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewStringValue("value"),
				},
			},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "wrong type - not struct",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("not a struct"),
				},
			},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "success with resource_type_id and resource_id",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"resource_type_id": structpb.NewStringValue("user"),
							"resource_id":      structpb.NewStringValue("123"),
						},
					}),
				},
			},
			key: "test",
			wantVal: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "123",
			},
			wantOk: true,
		},
		{
			name: "success with resource_type and resource (alternative)",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"resource_type": structpb.NewStringValue("group"),
							"resource":      structpb.NewStringValue("456"),
						},
					}),
				},
			},
			key: "test",
			wantVal: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "456",
			},
			wantOk: true,
		},
		{
			name: "missing resource_type_id",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"resource_id": structpb.NewStringValue("123"),
						},
					}),
				},
			},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "missing resource_id",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"resource_type_id": structpb.NewStringValue("user"),
						},
					}),
				},
			},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetResourceIDArg(tt.args, tt.key)
			require.Equal(t, tt.wantOk, ok)
			if tt.wantVal != nil {
				require.NotNil(t, val)
				require.Equal(t, tt.wantVal.ResourceType, val.ResourceType)
				require.Equal(t, tt.wantVal.Resource, val.Resource)
			} else {
				require.Nil(t, val)
			}
		})
	}
}

func TestGetStringSliceArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantVal []string
		wantOk  bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name:    "nil fields",
			args:    &structpb.Struct{},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{structpb.NewStringValue("a")},
					}),
				},
			},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "wrong type - not list",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("not a list"),
				},
			},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "wrong type - list with non-strings",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{structpb.NewNumberValue(123)},
					}),
				},
			},
			key:     "test",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "success",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("a"),
							structpb.NewStringValue("b"),
							structpb.NewStringValue("c"),
						},
					}),
				},
			},
			key:     "test",
			wantVal: []string{"a", "b", "c"},
			wantOk:  true,
		},
		{
			name: "empty list",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{},
					}),
				},
			},
			key:     "test",
			wantVal: []string{},
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetStringSliceArg(tt.args, tt.key)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.wantVal, val)
		})
	}
}

func TestGetStructArg(t *testing.T) {
	tests := []struct {
		name   string
		args   *structpb.Struct
		key    string
		wantOk bool
	}{
		{
			name:   "nil args",
			args:   nil,
			key:    "test",
			wantOk: false,
		},
		{
			name:   "nil fields",
			args:   &structpb.Struct{},
			key:    "test",
			wantOk: false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewStructValue(&structpb.Struct{}),
				},
			},
			key:    "test",
			wantOk: false,
		},
		{
			name: "wrong type",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("not a struct"),
				},
			},
			key:    "test",
			wantOk: false,
		},
		{
			name: "success",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"nested": structpb.NewStringValue("value"),
						},
					}),
				},
			},
			key:    "test",
			wantOk: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetStructArg(tt.args, tt.key)
			require.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				require.NotNil(t, val)
			} else {
				require.Nil(t, val)
			}
		})
	}
}

func TestRequireStringArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantVal string
		wantErr bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantVal: "",
			wantErr: true,
		},
		{
			name: "success",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("hello"),
				},
			},
			key:     "test",
			wantVal: "hello",
			wantErr: false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewStringValue("value"),
				},
			},
			key:     "test",
			wantVal: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := RequireStringArg(tt.args, tt.key)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantVal, val)
			}
		})
	}
}

func TestRequireResourceIDArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantVal *v2.ResourceId
		wantErr bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantVal: nil,
			wantErr: true,
		},
		{
			name: "success",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"resource_type_id": structpb.NewStringValue("user"),
							"resource_id":      structpb.NewStringValue("123"),
						},
					}),
				},
			},
			key: "test",
			wantVal: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "123",
			},
			wantErr: false,
		},
		{
			name: "key not found",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"other": structpb.NewStringValue("value"),
				},
			},
			key:     "test",
			wantVal: nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := RequireResourceIDArg(tt.args, tt.key)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, val)
				require.Equal(t, tt.wantVal.ResourceType, val.ResourceType)
				require.Equal(t, tt.wantVal.Resource, val.Resource)
			}
		})
	}
}

func TestGetResourceIdListArg(t *testing.T) {
	tests := []struct {
		name    string
		args    *structpb.Struct
		key     string
		wantLen int
		wantOk  bool
	}{
		{
			name:    "nil args",
			args:    nil,
			key:     "test",
			wantLen: 0,
			wantOk:  false,
		},
		{
			name: "success",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStructValue(&structpb.Struct{
								Fields: map[string]*structpb.Value{
									"resource_type_id": structpb.NewStringValue("user"),
									"resource_id":      structpb.NewStringValue("123"),
								},
							}),
							structpb.NewStructValue(&structpb.Struct{
								Fields: map[string]*structpb.Value{
									"resource_type_id": structpb.NewStringValue("group"),
									"resource_id":      structpb.NewStringValue("456"),
								},
							}),
						},
					}),
				},
			},
			key:     "test",
			wantLen: 2,
			wantOk:  true,
		},
		{
			name: "empty list",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{},
					}),
				},
			},
			key:     "test",
			wantLen: 0,
			wantOk:  true,
		},
		{
			name: "wrong type - not list",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewStringValue("not a list"),
				},
			},
			key:     "test",
			wantLen: 0,
			wantOk:  false,
		},
		{
			name: "invalid item in list",
			args: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"test": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("not a struct"),
						},
					}),
				},
			},
			key:     "test",
			wantLen: 0,
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := GetResourceIdListArg(tt.args, tt.key)
			require.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				require.Len(t, val, tt.wantLen)
			}
		})
	}
}

func TestSetResourceFieldArg(t *testing.T) {
	t.Run("nil args", func(t *testing.T) {
		err := SetResourceFieldArg(nil, "test", &v2.Resource{})
		require.Error(t, err)
	})

	t.Run("nil resource", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		err := SetResourceFieldArg(args, "test", nil)
		require.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		resource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "123",
			},
			DisplayName: "Test User",
			Description: "A test user",
		}

		err := SetResourceFieldArg(args, "test", resource)
		require.NoError(t, err)

		// Verify we can read it back
		retrieved, ok := GetResourceFieldArg(args, "test")
		require.True(t, ok)
		require.NotNil(t, retrieved)
		require.Equal(t, resource.Id.ResourceType, retrieved.Id.ResourceType)
		require.Equal(t, resource.Id.Resource, retrieved.Id.Resource)
		require.Equal(t, resource.DisplayName, retrieved.DisplayName)
		require.Equal(t, resource.Description, retrieved.Description)
	})

	t.Run("success with nil fields", func(t *testing.T) {
		args := &structpb.Struct{}
		resource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "123",
			},
		}

		err := SetResourceFieldArg(args, "test", resource)
		require.NoError(t, err)
		require.NotNil(t, args.Fields)
	})
}

func TestGetResourceFieldArg(t *testing.T) {
	t.Run("nil args", func(t *testing.T) {
		val, ok := GetResourceFieldArg(nil, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("key not found", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		val, ok := GetResourceFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("wrong type", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test": structpb.NewStringValue("not a struct"),
			},
		}
		val, ok := GetResourceFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("roundtrip", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		resource := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "456",
			},
			DisplayName: "Test Group",
			Description: "A test group",
		}

		err := SetResourceFieldArg(args, "myresource", resource)
		require.NoError(t, err)

		retrieved, ok := GetResourceFieldArg(args, "myresource")
		require.True(t, ok)
		require.NotNil(t, retrieved)
		require.Equal(t, "group", retrieved.Id.ResourceType)
		require.Equal(t, "456", retrieved.Id.Resource)
		require.Equal(t, "Test Group", retrieved.DisplayName)
		require.Equal(t, "A test group", retrieved.Description)
	})
}

func TestGetResourceListFieldArg(t *testing.T) {
	t.Run("nil args", func(t *testing.T) {
		val, ok := GetResourceListFieldArg(nil, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("key not found", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		val, ok := GetResourceListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("wrong type - not list", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test": structpb.NewStringValue("not a list"),
			},
		}
		val, ok := GetResourceListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})
}

func TestNewReturnValues(t *testing.T) {
	t.Run("success with no fields", func(t *testing.T) {
		rv := NewReturnValues(true)
		require.NotNil(t, rv)
		require.NotNil(t, rv.Fields)

		success, ok := GetBoolArg(rv, "success")
		require.True(t, ok)
		require.True(t, success)
	})

	t.Run("failure with no fields", func(t *testing.T) {
		rv := NewReturnValues(false)
		require.NotNil(t, rv)

		success, ok := GetBoolArg(rv, "success")
		require.True(t, ok)
		require.False(t, success)
	})

	t.Run("success with string field", func(t *testing.T) {
		rv := NewReturnValues(true, NewStringReturnField("message", "hello"))
		require.NotNil(t, rv)

		success, ok := GetBoolArg(rv, "success")
		require.True(t, ok)
		require.True(t, success)

		message, ok := GetStringArg(rv, "message")
		require.True(t, ok)
		require.Equal(t, "hello", message)
	})

	t.Run("success with multiple fields", func(t *testing.T) {
		rv := NewReturnValues(true,
			NewStringReturnField("name", "test"),
			NewNumberReturnField("count", 42),
			NewBoolReturnField("active", true),
		)
		require.NotNil(t, rv)

		name, ok := GetStringArg(rv, "name")
		require.True(t, ok)
		require.Equal(t, "test", name)

		count, ok := GetIntArg(rv, "count")
		require.True(t, ok)
		require.Equal(t, int64(42), count)

		active, ok := GetBoolArg(rv, "active")
		require.True(t, ok)
		require.True(t, active)
	})
}

func TestNewStringReturnField(t *testing.T) {
	field := NewStringReturnField("name", "value")
	require.Equal(t, "name", field.Key)
	require.NotNil(t, field.Value)

	stringVal, ok := field.Value.GetKind().(*structpb.Value_StringValue)
	require.True(t, ok)
	require.Equal(t, "value", stringVal.StringValue)
}

func TestNewBoolReturnField(t *testing.T) {
	field := NewBoolReturnField("active", true)
	require.Equal(t, "active", field.Key)
	require.NotNil(t, field.Value)

	boolVal, ok := field.Value.GetKind().(*structpb.Value_BoolValue)
	require.True(t, ok)
	require.True(t, boolVal.BoolValue)
}

func TestNewNumberReturnField(t *testing.T) {
	field := NewNumberReturnField("count", 3.14)
	require.Equal(t, "count", field.Key)
	require.NotNil(t, field.Value)

	numVal, ok := field.Value.GetKind().(*structpb.Value_NumberValue)
	require.True(t, ok)
	require.Equal(t, 3.14, numVal.NumberValue)
}

func TestNewResourceReturnField(t *testing.T) {
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "123",
		},
		DisplayName: "Test User",
	}

	field, err := NewResourceReturnField("user", resource)
	require.NoError(t, err)
	require.Equal(t, "user", field.Key)
	require.NotNil(t, field.Value)

	// Verify it's a struct value
	_, ok := field.Value.GetKind().(*structpb.Value_StructValue)
	require.True(t, ok)
}

func TestNewResourceIdReturnField(t *testing.T) {
	resourceId := &v2.ResourceId{
		ResourceType: "user",
		Resource:     "123",
	}

	field, err := NewResourceIdReturnField("user_id", resourceId)
	require.NoError(t, err)
	require.Equal(t, "user_id", field.Key)
	require.NotNil(t, field.Value)

	// Verify it's a struct value
	_, ok := field.Value.GetKind().(*structpb.Value_StructValue)
	require.True(t, ok)
}

func TestNewStringListReturnField(t *testing.T) {
	field := NewStringListReturnField("names", []string{"a", "b", "c"})
	require.Equal(t, "names", field.Key)
	require.NotNil(t, field.Value)

	listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
	require.True(t, ok)
	require.Len(t, listVal.ListValue.Values, 3)

	// Verify values
	for i, expected := range []string{"a", "b", "c"} {
		strVal, ok := listVal.ListValue.Values[i].GetKind().(*structpb.Value_StringValue)
		require.True(t, ok)
		require.Equal(t, expected, strVal.StringValue)
	}
}

func TestNewNumberListReturnField(t *testing.T) {
	field := NewNumberListReturnField("scores", []float64{1.0, 2.5, 3.7})
	require.Equal(t, "scores", field.Key)
	require.NotNil(t, field.Value)

	listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
	require.True(t, ok)
	require.Len(t, listVal.ListValue.Values, 3)

	// Verify values
	for i, expected := range []float64{1.0, 2.5, 3.7} {
		numVal, ok := listVal.ListValue.Values[i].GetKind().(*structpb.Value_NumberValue)
		require.True(t, ok)
		require.Equal(t, expected, numVal.NumberValue)
	}
}

func TestNewResourceListReturnField(t *testing.T) {
	resources := []*v2.Resource{
		{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "1",
			},
			DisplayName: "User 1",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "2",
			},
			DisplayName: "User 2",
		},
	}

	field, err := NewResourceListReturnField("users", resources)
	require.NoError(t, err)
	require.Equal(t, "users", field.Key)
	require.NotNil(t, field.Value)

	listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
	require.True(t, ok)
	require.Len(t, listVal.ListValue.Values, 2)
}

func TestNewResourceIdListReturnField(t *testing.T) {
	resourceIds := []*v2.ResourceId{
		{ResourceType: "user", Resource: "1"},
		{ResourceType: "user", Resource: "2"},
		{ResourceType: "group", Resource: "3"},
	}

	field, err := NewResourceIdListReturnField("ids", resourceIds)
	require.NoError(t, err)
	require.Equal(t, "ids", field.Key)
	require.NotNil(t, field.Value)

	listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
	require.True(t, ok)
	require.Len(t, listVal.ListValue.Values, 3)
}

func TestNewListReturnField(t *testing.T) {
	values := []*structpb.Value{
		structpb.NewStringValue("a"),
		structpb.NewNumberValue(42),
		structpb.NewBoolValue(true),
	}

	field := NewListReturnField("mixed", values)
	require.Equal(t, "mixed", field.Key)
	require.NotNil(t, field.Value)

	listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
	require.True(t, ok)
	require.Len(t, listVal.ListValue.Values, 3)
}

func TestNewReturnField(t *testing.T) {
	value := structpb.NewStringValue("test")
	field := NewReturnField("custom", value)
	require.Equal(t, "custom", field.Key)
	require.Equal(t, value, field.Value)
}

func TestRequireResourceIdListArg(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test": structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStructValue(&structpb.Struct{
							Fields: map[string]*structpb.Value{
								"resource_type_id": structpb.NewStringValue("user"),
								"resource_id":      structpb.NewStringValue("123"),
							},
						}),
					},
				}),
			},
		}

		val, err := RequireResourceIdListArg(args, "test")
		require.NoError(t, err)
		require.Len(t, val, 1)
		require.Equal(t, "user", val[0].ResourceType)
		require.Equal(t, "123", val[0].Resource)
	})

	t.Run("missing key", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		val, err := RequireResourceIdListArg(args, "test")
		require.Error(t, err)
		require.Nil(t, val)
	})
}

func TestResourceConversionHelpers(t *testing.T) {
	t.Run("resourceToBasicResource and back", func(t *testing.T) {
		original := &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "test-123",
			},
			DisplayName: "Test User",
			Description: "A test user description",
		}

		basic := resourceToBasicResource(original)
		require.NotNil(t, basic)
		require.Equal(t, "user", basic.ResourceId.ResourceTypeId)
		require.Equal(t, "test-123", basic.ResourceId.ResourceId)
		require.Equal(t, "Test User", basic.DisplayName)
		require.Equal(t, "A test user description", basic.Description)

		converted := basicResourceToResource(basic)
		require.NotNil(t, converted)
		require.Equal(t, original.Id.ResourceType, converted.Id.ResourceType)
		require.Equal(t, original.Id.Resource, converted.Id.Resource)
		require.Equal(t, original.DisplayName, converted.DisplayName)
		require.Equal(t, original.Description, converted.Description)
	})
}
