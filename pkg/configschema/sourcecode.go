package configschema

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
)

// findSchemaConfigFunction searches for `SchemaConfig() []SchemaField`.
func findSchemaConfigFunction(filePath string) (bool, error) {
	return findFunctionWithReturn(filePath, "SchemaConfig", "SchemaField")
}

// findSchemaRelationshipFunction searches for `SchemaRelationship() []SchemaFieldRelationship`.
func findSchemaRelationshipFunction(filepath string) (bool, error) {
	return findFunctionWithReturn(filepath, "SchemaRelationship", "SchemaFieldRelationship")
}

// findFunctionWithReturn parses the source searching for a function
// the function has to match in name and return.
func findFunctionWithReturn(filePath string, funcName, returnName string) (bool, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, nil, parser.AllErrors)
	if err != nil {
		return false, err
	}

	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if ok && fn.Name.Name == funcName {
			if fn.Type.Results != nil && len(fn.Type.Results.List) == 1 {
				resultType, ok := fn.Type.Results.List[0].Type.(*ast.ArrayType)
				if ok {
					if ident, ok := resultType.Elt.(*ast.SelectorExpr); ok {
						if ident.Sel.Name == returnName {
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

// copyGoFileToTmpMainPackage modify the package name of a file and copies it to a new location
func copyGoFileToTmpMainPackage(originalFilePath, outputdir string) (string, error) {
	fset := token.NewFileSet()

	node, err := parser.ParseFile(fset, originalFilePath, nil, parser.AllErrors)
	if err != nil {
		return "", err
	}

	node.Name.Name = "main"

	newfile := filepath.Join(outputdir, "plugin.go")

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
