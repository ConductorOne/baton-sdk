package configschema

import (
	"fmt"
)

// LoadAndEnsureDefaultFields compiles a go file with a configuration schema and return its definition
// after checking the default fields of `defaultFields` are set.
func LoadAndEnsureDefaultFields(filePath string) ([]ConfigField, error) {
	originalFields, err := Load(filePath)
	if err != nil {
		return nil, err
	}

	return ensureDefaultFieldsExists(originalFields), nil
}

// Load compiles a go file with a configuration schema and return its definition.
func Load(filePath string) ([]ConfigField, error) {
	fileLocation, err := createMainPackageFileIfNeeded(filePath)
	if err != nil {
		return nil, err
	}

	found, err := findSchemaConfigFunction(fileLocation)
	if !found {
		return nil, fmt.Errorf("SchemaConfig function not found at '%s'", filePath)
	}

	if err != nil {
		return nil, fmt.Errorf(
			"unable to search for SchemaConfig function at '%s', error: %w",
			filePath,
			err,
		)
	}

	pluginLocation, err := compileAndLoadPlugin(fileLocation)
	if err != nil {
		return nil, fmt.Errorf("unable to compile file '%s', error: %w", filePath, err)
	}

	fields, err := loadAndExecutePlugin(pluginLocation)
	if err != nil {
		return nil, err
	}

	return fields, nil
}
