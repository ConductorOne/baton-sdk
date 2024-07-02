package configschema

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
)

// findSchemaConfigFunction parses the source searching for `SchemaConfig`.
// the function should be `SchemaConfig() []SchemaField`.
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
						if ident.Sel.Name == "SchemaField" {
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

// createMainPackageFileIfNeeded modify the package name of a file.
// this happens in another file so we don't overwrite the original with gibberish.
func getFilePackageName(originalFilePath string) (string, error) {
	fset := token.NewFileSet()

	node, err := parser.ParseFile(fset, originalFilePath, nil, parser.AllErrors)
	if err != nil {
		return "", err
	}

	return node.Name.Name, nil
}
