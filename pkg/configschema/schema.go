package configschema

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"reflect"
)

type ConfigField struct {
	FieldName    string
	FieldType    reflect.Kind
	Required     bool
	Description  string
	DefaultValue any
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
		return nil, fmt.Errorf("unable to search for SchemaConfig function at '%s', error: %w", filePath, err)
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

// findSchemaConfigFunction parses the source searching for `SchemaConfig`.
// the function should be `SchemaConfig() []ConfigField`.
func findSchemaConfigFunction(filePath string) (bool, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, nil, parser.AllErrors)
	if err != nil {
		return false, err
	}

	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if ok && fn.Name.Name == "SchemaConfig" {
			if fn.Type.Results != nil && len(fn.Type.Results.List) == 1 {
				resultType, ok := fn.Type.Results.List[0].Type.(*ast.ArrayType)
				if ok {
					if ident, ok := resultType.Elt.(*ast.SelectorExpr); ok {
						if ident.Sel.Name == "ConfigField" {
							found = true
							return false
						}
					}
				}
			}
		}
		return true
	})

	return found, nil
}

// createMainPackageFileIfNeeded modify the package name of a file.
// this happens in another file so we don't overwrite the original with gibberish.
func createMainPackageFileIfNeeded(originalFilePath string) (string, error) {
	fset := token.NewFileSet()

	node, err := parser.ParseFile(fset, originalFilePath, nil, parser.AllErrors)
	if err != nil {
		return "", err
	}

	// return original file path if the file already is in the main package
	if node.Name.Name == "main" {
		return originalFilePath, nil
	}

	node.Name.Name = "main"

	pkgDir, err := os.MkdirTemp("", "baton-sdk-schema-*")
	if err != nil {
		return "", fmt.Errorf("unable to create temporary directory, error: %w", err)
	}

	newfile := filepath.Join(pkgDir, "plugin.go")

	fd, err := os.OpenFile(newfile, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}

	if err := printer.Fprint(fd, fset, node); err != nil {
		return "", err
	}
	defer fd.Close()

	return newfile, nil
}
