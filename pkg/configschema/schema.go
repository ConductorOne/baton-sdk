package configschema

import (
	"fmt"
	"os"
	"path/filepath"
)

// ReadSchemaAndGenerateFiles generates source code in a path.
// This function takes a path to a file containing the schema of the
// configuration and output a configuration file and a configuration for a
// command-line interface using Cobra.
func ReadSchemaAndGenerateFiles(schemaPath string) error {
	absschemapath, err := filepath.Abs(schemaPath)
	if err != nil {
		return err
	}
	originalPackageName, err := getFilePackageName(absschemapath)
	if err != nil {
		return err
	}

	fields, err := LoadAndEnsureDefaultFields(absschemapath)
	if err != nil {
		return err
	}

	// configuration.go source generation
	configpath := filepath.Join(filepath.Dir(absschemapath), "configuration.go")
	fdconfig, err := os.OpenFile(configpath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer fdconfig.Close()

	templateData := TemplateData{PackageName: originalPackageName, Fields: fields}
	err = RenderConfig(templateData, fdconfig)
	if err != nil {
		return err
	}

	// cli.go source generation
	clipath := filepath.Join(filepath.Dir(absschemapath), "cli.go")
	fdcli, err := os.OpenFile(clipath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer fdcli.Close()

	err = RenderCLI(templateData, fdcli)
	if err != nil {
		return err
	}

	return nil
}

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
