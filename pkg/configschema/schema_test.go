package configschema_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/configschema"
	"github.com/stretchr/testify/require"
)

var (
	example = []byte(`package main

import (
	"github.com/conductorone/baton-sdk/pkg/configschema"
	"reflect"
)

func SchemaConfig() []configschema.ConfigField {
	return []configschema.ConfigField{
		{
			FieldName:    "jorge",
			FieldType:    reflect.Bool,
			Required:     false,
			Description:  "indicate wherever Jorge is false or true",
			DefaultValue: false,
		},
	}
}`)

	exampleAnotherPackage = []byte(`package randompackage

import (
	"github.com/conductorone/baton-sdk/pkg/configschema"
	"reflect"
)

func SchemaConfig() []configschema.ConfigField {
	return []configschema.ConfigField{
		{
			FieldName:    "jorge",
			FieldType:    reflect.Bool,
			Required:     false,
			Description:  "indicate wherever Jorge is false or true",
			DefaultValue: false,
		},
	}
}`)

	exampleOfBadReturn = []byte(`package main

func SchemaConfig() int {
	return 1
}`)
)

type schemaConfigTestCase struct {
	name   string
	input  schemaConfigTestInput
	expect schemaConfigTestExpect
}

type schemaConfigTestInput struct {
	goSourceCode []byte
	fieldIndex   int
	fieldName    string
}

type schemaConfigTestExpect struct {
	err string
}

func TestLoadx(t *testing.T) {
	testCases := []schemaConfigTestCase{
		{
			name: "success - load schema with file in `package main`",
			input: schemaConfigTestInput{
				goSourceCode: example,
				fieldName:    "jorge",
			},
		},
		{
			name: "success - load schema with a file in `package randompackage`",
			input: schemaConfigTestInput{
				goSourceCode: exampleAnotherPackage,
				fieldName:    "jorge",
			},
		},
		{
			name: "failure - cannot load schema from an empty file",
			input: schemaConfigTestInput{
				goSourceCode: []byte(``),
			},
			expect: schemaConfigTestExpect{
				err: "expected ';', found 'EOF' (and 2 more errors)",
			},
		},
		{
			name: "failure - cannot load schema from a file that only has a `package` statement",
			input: schemaConfigTestInput{
				goSourceCode: []byte(`package main`),
			},
			expect: schemaConfigTestExpect{
				err: "SchemaConfig function not found",
			},
		},
		{
			name: "failure - cannot load schema from incorrect function return",
			input: schemaConfigTestInput{
				goSourceCode: exampleOfBadReturn,
			},
			expect: schemaConfigTestExpect{
				err: "SchemaConfig function not found",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rootDir, err := os.MkdirTemp("", "baton-sdk-configschema-test-*")
			require.NoError(t, err)

			goFilePath := filepath.Join(rootDir, "example.go")
			if err := os.WriteFile(goFilePath, tc.input.goSourceCode, 0600); err != nil {
				t.Fatalf("unable to write the go file for testing: %v", err)
			}

			defer os.RemoveAll(rootDir)

			fields, err := configschema.Load(goFilePath)
			if tc.expect.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expect.err)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, fields)
				// this is just to be sure that we got the same output as the input
				require.Equal(t, tc.input.fieldName, fields[tc.input.fieldIndex].FieldName)
			}
		})
	}
}