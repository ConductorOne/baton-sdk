package configschema

import (
	"bytes"
	"fmt"
	"os/exec"
	"plugin"
)

// compileAndLoadPlugin compiles a go source as a plugin.
func compileAndLoadPlugin(filePath string) (string, error) {
	pluginPath := filePath[:len(filePath)-3] + ".so"
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", pluginPath, filePath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to compile plugin: %s", stderr.String())
	}
	return pluginPath, nil
}

// loadAndExecutePlugin load and execute the plugin function.
func loadAndExecutePlugin(pluginPath string) ([]ConfigField, error) {
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin: %w", err)
	}

	symbol, err := p.Lookup("SchemaConfig")
	if err != nil {
		return nil, fmt.Errorf("failed to find SchemaConfig function: %w", err)
	}

	schemaConfigFunc, ok := symbol.(func() []ConfigField)
	if !ok {
		return nil, fmt.Errorf("SchemaConfig has wrong type signature")
	}

	return schemaConfigFunc(), nil
}
