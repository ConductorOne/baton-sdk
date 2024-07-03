package configschema

import (
	"bytes"
	"fmt"
	"os/exec"
	"plugin"
)

// compileAndLoadPlugin compiles a go source as a plugin.
func compileAndLoadPlugin(filePath string, cwd string) (string, error) {
	pluginPath := filePath[:len(filePath)-3] + ".so"
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", pluginPath, filePath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// change CWD if needed
	if cwd != "" {
		cmd.Dir = cwd
	}
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to compile plugin: %s", stderr.String())
	}
	return pluginPath, nil
}

// loadAndExecutePluginForSchemaConfig load and execute the plugin function.
func loadAndExecutePluginForSchemaConfig(pluginPath string) ([]SchemaField, error) {
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin: %w", err)
	}

	symbol, err := p.Lookup("SchemaConfig")
	if err != nil {
		return nil, fmt.Errorf("failed to find SchemaConfig function: %w", err)
	}

	ourFunc, ok := symbol.(func() []SchemaField)
	if !ok {
		return nil, fmt.Errorf("SchemaConfig has wrong type signature")
	}

	return ourFunc(), nil
}

// loadAndExecutePluginForSchemaFieldsRelationship load and execute the plugin function.
func loadAndExecutePluginForSchemaFieldsRelationship(pluginPath string) ([]SchemaFieldRelationship, error) {
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin: %w", err)
	}

	symbol, err := p.Lookup("SchemaFieldsRelationship")
	if err != nil {
		return nil, fmt.Errorf("failed to find SchemaFieldsRelationship function: %w", err)
	}

	ourFunc, ok := symbol.(func() []SchemaFieldRelationship)
	if !ok {
		return nil, fmt.Errorf("SchemaFieldsRelationship has wrong type signature")
	}

	return ourFunc(), nil
}
