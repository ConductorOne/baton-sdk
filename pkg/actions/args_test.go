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
	resourceIDs := []*v2.ResourceId{
		{ResourceType: "user", Resource: "1"},
		{ResourceType: "user", Resource: "2"},
		{ResourceType: "group", Resource: "3"},
	}

	field, err := NewResourceIdListReturnField("ids", resourceIDs)
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
		require.Equal(t, "user", basic.GetResourceId().GetResourceTypeId())
		require.Equal(t, "test-123", basic.GetResourceId().GetResourceId())
		require.Equal(t, "Test User", basic.GetDisplayName())
		require.Equal(t, "A test user description", basic.GetDescription())

		converted := basicResourceToResource(basic)
		require.NotNil(t, converted)
		require.Equal(t, original.Id.ResourceType, converted.Id.ResourceType)
		require.Equal(t, original.Id.Resource, converted.Id.Resource)
		require.Equal(t, original.DisplayName, converted.DisplayName)
		require.Equal(t, original.Description, converted.Description)
	})
}

func TestEntitlementConversionHelpers(t *testing.T) {
	t.Run("entitlementToBasicEntitlement and back", func(t *testing.T) {
		original := &v2.Entitlement{
			Id:          "entitlement-123",
			DisplayName: "Test Entitlement",
			Description: "A test entitlement description",
			Slug:        "test-entitlement",
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
			GrantableTo: []*v2.ResourceType{
				{Id: "user"},
				{Id: "group"},
			},
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "role",
					Resource:     "admin",
				},
			},
		}

		basic := entitlementToBasicEntitlement(original)
		require.NotNil(t, basic)
		require.Equal(t, "entitlement-123", basic.GetId())
		require.Equal(t, "Test Entitlement", basic.GetDisplayName())
		require.Equal(t, "A test entitlement description", basic.GetDescription())
		require.Equal(t, "test-entitlement", basic.GetSlug())
		require.Equal(t, "PURPOSE_VALUE_ASSIGNMENT", basic.GetPurpose())
		require.Equal(t, []string{"user", "group"}, basic.GetGrantableToResourceTypeIds())
		require.Equal(t, "admin", basic.GetResourceId())
		require.Equal(t, "role", basic.GetResourceTypeId())

		converted := basicEntitlementToEntitlement(basic)
		require.NotNil(t, converted)
		require.Equal(t, original.Id, converted.Id)
		require.Equal(t, original.DisplayName, converted.DisplayName)
		require.Equal(t, original.Description, converted.Description)
		require.Equal(t, original.Slug, converted.Slug)
		require.Equal(t, original.Purpose, converted.Purpose)
		require.Len(t, converted.GrantableTo, 2)
		require.Equal(t, "user", converted.GrantableTo[0].Id)
		require.Equal(t, "group", converted.GrantableTo[1].Id)
		require.NotNil(t, converted.Resource)
		require.Equal(t, "admin", converted.Resource.Id.Resource)
		require.Equal(t, "role", converted.Resource.Id.ResourceType)
	})

	t.Run("handles nil resource", func(t *testing.T) {
		original := &v2.Entitlement{
			Id:          "entitlement-123",
			DisplayName: "Test Entitlement",
			Resource:    nil,
		}

		basic := entitlementToBasicEntitlement(original)
		require.NotNil(t, basic)
		require.Empty(t, basic.GetResourceId())
		require.Empty(t, basic.GetResourceTypeId())

		converted := basicEntitlementToEntitlement(basic)
		require.NotNil(t, converted)
		require.Nil(t, converted.Resource)
	})

	t.Run("handles all purpose values", func(t *testing.T) {
		purposes := []v2.Entitlement_PurposeValue{
			v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED,
			v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
			v2.Entitlement_PURPOSE_VALUE_PERMISSION,
			v2.Entitlement_PURPOSE_VALUE_OWNERSHIP,
		}

		for _, purpose := range purposes {
			original := &v2.Entitlement{
				Id:      "ent-1",
				Purpose: purpose,
			}

			basic := entitlementToBasicEntitlement(original)
			converted := basicEntitlementToEntitlement(basic)
			require.Equal(t, purpose, converted.Purpose)
		}
	})
}

func TestGrantConversionHelpers(t *testing.T) {
	t.Run("grantToBasicGrant and back", func(t *testing.T) {
		original := &v2.Grant{
			Id: "grant-123",
			Entitlement: &v2.Entitlement{
				Id: "entitlement-456",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "user-789",
				},
				DisplayName: "Test User",
			},
		}

		basic := grantToBasicGrant(original)
		require.NotNil(t, basic)
		require.Equal(t, "grant-123", basic.GetId())
		require.NotNil(t, basic.GetEntitlement())
		require.Equal(t, "entitlement-456", basic.GetEntitlement().GetId())
		require.NotNil(t, basic.GetPrincipal())

		converted := basicGrantToGrant(basic)
		require.NotNil(t, converted)
		require.Equal(t, original.Id, converted.Id)
		require.NotNil(t, converted.Entitlement)
		require.Equal(t, "entitlement-456", converted.Entitlement.Id)
		require.NotNil(t, converted.Principal)
		require.Equal(t, "user", converted.Principal.Id.ResourceType)
		require.Equal(t, "user-789", converted.Principal.Id.Resource)
	})

	t.Run("handles nil entitlement", func(t *testing.T) {
		original := &v2.Grant{
			Id:          "grant-123",
			Entitlement: nil,
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "user-123",
				},
			},
		}

		basic := grantToBasicGrant(original)
		require.NotNil(t, basic)
		require.Nil(t, basic.GetEntitlement())

		converted := basicGrantToGrant(basic)
		require.NotNil(t, converted)
		require.Nil(t, converted.Entitlement)
	})

	t.Run("handles nil principal", func(t *testing.T) {
		original := &v2.Grant{
			Id: "grant-123",
			Entitlement: &v2.Entitlement{
				Id: "entitlement-456",
			},
			Principal: nil,
		}

		basic := grantToBasicGrant(original)
		require.NotNil(t, basic)
		require.Nil(t, basic.GetPrincipal())

		converted := basicGrantToGrant(basic)
		require.NotNil(t, converted)
		require.Nil(t, converted.Principal)
	})
}

func TestGetEntitlementListFieldArg(t *testing.T) {
	t.Run("nil args", func(t *testing.T) {
		val, ok := GetEntitlementListFieldArg(nil, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("nil fields", func(t *testing.T) {
		val, ok := GetEntitlementListFieldArg(&structpb.Struct{}, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("key not found", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		val, ok := GetEntitlementListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("wrong type - not list", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test": structpb.NewStringValue("not a list"),
			},
		}
		val, ok := GetEntitlementListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("wrong type - list with non-struct", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test": structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("not a struct"),
					},
				}),
			},
		}
		val, ok := GetEntitlementListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("roundtrip with NewEntitlementListReturnField", func(t *testing.T) {
		entitlements := []*v2.Entitlement{
			{
				Id:          "ent-1",
				DisplayName: "Entitlement 1",
				Description: "First entitlement",
				Slug:        "ent-1",
				Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
				GrantableTo: []*v2.ResourceType{{Id: "user"}},
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "role",
						Resource:     "admin",
					},
				},
			},
			{
				Id:          "ent-2",
				DisplayName: "Entitlement 2",
				Description: "Second entitlement",
				Slug:        "ent-2",
				Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
			},
		}

		field, err := NewEntitlementListReturnField("entitlements", entitlements)
		require.NoError(t, err)

		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"entitlements": field.Value,
			},
		}

		retrieved, ok := GetEntitlementListFieldArg(args, "entitlements")
		require.True(t, ok)
		require.Len(t, retrieved, 2)

		require.Equal(t, "ent-1", retrieved[0].Id)
		require.Equal(t, "Entitlement 1", retrieved[0].DisplayName)
		require.Equal(t, v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT, retrieved[0].Purpose)
		require.Len(t, retrieved[0].GrantableTo, 1)
		require.NotNil(t, retrieved[0].Resource)

		require.Equal(t, "ent-2", retrieved[1].Id)
		require.Equal(t, "Entitlement 2", retrieved[1].DisplayName)
		require.Equal(t, v2.Entitlement_PURPOSE_VALUE_PERMISSION, retrieved[1].Purpose)
	})

	t.Run("empty list", func(t *testing.T) {
		field, err := NewEntitlementListReturnField("entitlements", []*v2.Entitlement{})
		require.NoError(t, err)

		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"entitlements": field.Value,
			},
		}

		retrieved, ok := GetEntitlementListFieldArg(args, "entitlements")
		require.True(t, ok)
		require.Len(t, retrieved, 0)
	})
}

func TestGetGrantListFieldArg(t *testing.T) {
	t.Run("nil args", func(t *testing.T) {
		val, ok := GetGrantListFieldArg(nil, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("nil fields", func(t *testing.T) {
		val, ok := GetGrantListFieldArg(&structpb.Struct{}, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("key not found", func(t *testing.T) {
		args := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		val, ok := GetGrantListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("wrong type - not list", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test": structpb.NewStringValue("not a list"),
			},
		}
		val, ok := GetGrantListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("wrong type - list with non-struct", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"test": structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("not a struct"),
					},
				}),
			},
		}
		val, ok := GetGrantListFieldArg(args, "test")
		require.False(t, ok)
		require.Nil(t, val)
	})

	t.Run("roundtrip with NewGrantListReturnField", func(t *testing.T) {
		grants := []*v2.Grant{
			{
				Id: "grant-1",
				Entitlement: &v2.Entitlement{
					Id: "ent-1",
				},
				Principal: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "user",
						Resource:     "user-1",
					},
					DisplayName: "User 1",
				},
			},
			{
				Id: "grant-2",
				Entitlement: &v2.Entitlement{
					Id: "ent-2",
				},
				Principal: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "group",
						Resource:     "group-1",
					},
					DisplayName: "Group 1",
				},
			},
		}

		field, err := NewGrantListReturnField("grants", grants)
		require.NoError(t, err)

		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"grants": field.Value,
			},
		}

		retrieved, ok := GetGrantListFieldArg(args, "grants")
		require.True(t, ok)
		require.Len(t, retrieved, 2)

		require.Equal(t, "grant-1", retrieved[0].Id)
		require.NotNil(t, retrieved[0].Entitlement)
		require.Equal(t, "ent-1", retrieved[0].Entitlement.Id)
		require.NotNil(t, retrieved[0].Principal)
		require.Equal(t, "user", retrieved[0].Principal.Id.ResourceType)

		require.Equal(t, "grant-2", retrieved[1].Id)
		require.NotNil(t, retrieved[1].Entitlement)
		require.Equal(t, "ent-2", retrieved[1].Entitlement.Id)
		require.NotNil(t, retrieved[1].Principal)
		require.Equal(t, "group", retrieved[1].Principal.Id.ResourceType)
	})

	t.Run("empty list", func(t *testing.T) {
		field, err := NewGrantListReturnField("grants", []*v2.Grant{})
		require.NoError(t, err)

		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"grants": field.Value,
			},
		}

		retrieved, ok := GetGrantListFieldArg(args, "grants")
		require.True(t, ok)
		require.Len(t, retrieved, 0)
	})
}

func TestNewEntitlementListReturnField(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		entitlements := []*v2.Entitlement{
			{
				Id:          "ent-1",
				DisplayName: "Entitlement 1",
			},
			{
				Id:          "ent-2",
				DisplayName: "Entitlement 2",
			},
		}

		field, err := NewEntitlementListReturnField("entitlements", entitlements)
		require.NoError(t, err)
		require.Equal(t, "entitlements", field.Key)
		require.NotNil(t, field.Value)

		listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
		require.True(t, ok)
		require.Len(t, listVal.ListValue.Values, 2)
	})

	t.Run("nil entitlement in list", func(t *testing.T) {
		entitlements := []*v2.Entitlement{
			{Id: "ent-1"},
			nil,
		}

		field, err := NewEntitlementListReturnField("entitlements", entitlements)
		require.Error(t, err)
		require.Contains(t, err.Error(), "entitlement at index 1 cannot be nil")
		require.Empty(t, field.Key)
	})

	t.Run("empty list", func(t *testing.T) {
		field, err := NewEntitlementListReturnField("entitlements", []*v2.Entitlement{})
		require.NoError(t, err)
		require.Equal(t, "entitlements", field.Key)

		listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
		require.True(t, ok)
		require.Len(t, listVal.ListValue.Values, 0)
	})
}

func TestNewGrantListReturnField(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		grants := []*v2.Grant{
			{
				Id: "grant-1",
				Entitlement: &v2.Entitlement{
					Id: "ent-1",
				},
			},
			{
				Id: "grant-2",
				Entitlement: &v2.Entitlement{
					Id: "ent-2",
				},
			},
		}

		field, err := NewGrantListReturnField("grants", grants)
		require.NoError(t, err)
		require.Equal(t, "grants", field.Key)
		require.NotNil(t, field.Value)

		listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
		require.True(t, ok)
		require.Len(t, listVal.ListValue.Values, 2)
	})

	t.Run("nil grant in list", func(t *testing.T) {
		grants := []*v2.Grant{
			{Id: "grant-1"},
			nil,
		}

		field, err := NewGrantListReturnField("grants", grants)
		require.Error(t, err)
		require.Contains(t, err.Error(), "grant at index 1 cannot be nil")
		require.Empty(t, field.Key)
	})

	t.Run("empty list", func(t *testing.T) {
		field, err := NewGrantListReturnField("grants", []*v2.Grant{})
		require.NoError(t, err)
		require.Equal(t, "grants", field.Key)

		listVal, ok := field.Value.GetKind().(*structpb.Value_ListValue)
		require.True(t, ok)
		require.Len(t, listVal.ListValue.Values, 0)
	})
}
