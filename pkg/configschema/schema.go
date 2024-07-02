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

	fields, relationships, err := LoadAndEnsureDefaultFields(absschemapath)
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

	templateData := TemplateData{
		PackageName:   originalPackageName,
		Fields:        fields,
		Relationships: relationships,
	}
	err = renderConfig(templateData, fdconfig)
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

	err = renderCLI(templateData, fdcli)
	if err != nil {
		return err
	}

	return nil
}

// LoadAndEnsureDefaultFields compiles a go file with a configuration schema and return its definition
// after checking the default fields of `defaultFields` are set.
func LoadAndEnsureDefaultFields(filePath string) ([]SchemaField, []SchemaFieldRelationship, error) {
	originalFields, originalRelationships, err := load(filePath)
	if err != nil {
		return nil, nil, err
	}

	return ensureDefaultFieldsExists(originalFields), ensureDefaultRelationships(originalRelationships), nil
}

// load compiles a go file with a configuration schema and return its definition.
func load(filePath string) ([]SchemaField, []SchemaFieldRelationship, error) {
	fileLocation, err := createMainPackageFileIfNeeded(filePath)
	if err != nil {
		return nil, nil, err
	}

	found, err := findSchemaConfigFunction(fileLocation)
	if !found {
		return nil, nil, fmt.Errorf("SchemaConfig function not found at '%s'", filePath)
	}

	if err != nil {
		return nil, nil, fmt.Errorf(
			"unable to search for SchemaConfig function at '%s', error: %w",
			filePath,
			err,
		)
	}

	pluginLocation, err := compileAndLoadPlugin(fileLocation)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to compile file '%s', error: %w", filePath, err)
	}

	fields, err := loadAndExecutePluginForSchemaConfig(pluginLocation)
	if err != nil {
		return nil, nil, err
	}

	// fields relationships
	found, err = findSchemaRelationshipFunction(fileLocation)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"unable to search for SchemaRelationship function at '%s', error: %w",
			filePath,
			err,
		)
	}
	if !found {
		// since this definition is optional for the user, return empty
		return fields, nil, nil
	}

	relationships, err := loadAndExecutePluginForSchemaFieldsRelationship(pluginLocation)
	if err != nil {
		return nil, nil, err
	}

	return fields, relationships, nil
}
