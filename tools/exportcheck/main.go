// exportcheck checks the scoped packages for two things a reviewer can't
// see in a diff. Exported identifiers that no non-test code outside their
// own package refers to (satisfying an interface declared anywhere in the
// module or its imports counts). And type size: a struct over
// maxStructMethods methods or maxStructFields fields, or an interface over
// maxInterfaceMethods, is pinned at its baselined count and may only
// shrink; a type under the limits may grow up to them. A committed
// baseline holds the accepted exports and the pinned sizes; the check
// fails on additions to either, and -update rewrites it.
//
//	go run -C tools/exportcheck . -dir ../.. -scope ./pkg/dotc1z/...,./pkg/synccompactor/... -baseline ../../.exportcheck-baseline
//	go run -C tools/exportcheck . ... -update   # rewrite the baseline
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

func main() {
	dir := flag.String("dir", ".", "module root")
	scope := flag.String("scope", "", "comma-separated package patterns whose exports are checked")
	baseline := flag.String("baseline", "", "baseline file of accepted names")
	update := flag.Bool("update", false, "rewrite the baseline from the current result")
	flag.Parse()
	if *scope == "" || *baseline == "" {
		fmt.Fprintln(os.Stderr, "exportcheck: -scope and -baseline are required")
		os.Exit(2)
	}

	cfg := &packages.Config{
		Dir:   *dir,
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Tests: false,
	}
	all, err := packages.Load(cfg, "./...")
	if err != nil {
		fmt.Fprintln(os.Stderr, "exportcheck:", err)
		os.Exit(2)
	}
	if packages.PrintErrors(all) > 0 {
		os.Exit(2)
	}
	scoped, err := packages.Load(&packages.Config{Dir: *dir, Mode: packages.NeedName, Tests: false}, strings.Split(*scope, ",")...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "exportcheck:", err)
		os.Exit(2)
	}
	inScope := map[string]bool{}
	for _, p := range scoped {
		inScope[p.PkgPath] = true
	}

	unused := findUnreferenced(all, inScope)
	sizes := oversizedTypes(all, inScope)
	unused = append(unused, sizes...)
	sort.Strings(unused)

	if *update {
		if err := os.WriteFile(*baseline, []byte(strings.Join(unused, "\n")+"\n"), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "exportcheck:", err)
			os.Exit(2)
		}
		fmt.Printf("exportcheck: baseline written, %d names\n", len(unused))
		return
	}

	accepted := map[string]bool{}
	if data, err := os.ReadFile(*baseline); err == nil {
		for _, l := range strings.Split(string(data), "\n") {
			if l = strings.TrimSpace(l); l != "" {
				accepted[l] = true
			}
		}
	}
	var fresh []string
	for _, n := range unused {
		if accepted[n] {
			continue
		}
		if size, ok := parseSizeLine(n); ok && sizeShrank(accepted, size) {
			continue
		}
		fresh = append(fresh, n)
	}
	if len(fresh) == 0 {
		fmt.Printf("exportcheck: ok (%d accepted)\n", len(unused))
		return
	}
	fmt.Println("exportcheck: new findings (fix, or accept with `make exportcheck-update` and say why in the PR):")
	for _, n := range fresh {
		fmt.Println("  " + n)
	}
	os.Exit(1)
}

const (
	maxStructMethods    = 20
	maxStructFields     = 15
	maxInterfaceMethods = 10
)

// oversizedTypes returns "size pkgpath.Type methods=N fields=M" for every
// hand-written named type in scope over a limit. Generated types track
// their schema and are skipped; so is the field count of a struct with no
// methods, which is a record, not an abstraction.
func oversizedTypes(all []*packages.Package, inScope map[string]bool) []string {
	var out []string
	for _, p := range all {
		if !inScope[p.PkgPath] || p.Types == nil || p.Name == "main" {
			continue
		}
		generated := generatedFiles(p)
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || generated[p.Fset.Position(tn.Pos()).Filename] {
				continue
			}
			named, ok := tn.Type().(*types.Named)
			if !ok {
				continue
			}
			methods, fields, over := 0, 0, false
			switch u := named.Underlying().(type) {
			case *types.Interface:
				methods = u.NumMethods()
				over = methods > maxInterfaceMethods
			case *types.Struct:
				methods = named.NumMethods()
				fields = u.NumFields()
				over = methods > maxStructMethods || (methods > 0 && fields > maxStructFields)
			default:
				continue
			}
			if over {
				out = append(out, sizeLine(p.PkgPath+"."+name, methods, fields))
			}
		}
	}
	return out
}

type typeSize struct {
	name            string
	methods, fields int
}

func sizeLine(name string, methods, fields int) string {
	return fmt.Sprintf("size %s methods=%d fields=%d", name, methods, fields)
}

func parseSizeLine(l string) (typeSize, bool) {
	var s typeSize
	if _, err := fmt.Sscanf(l, "size %s methods=%d fields=%d", &s.name, &s.methods, &s.fields); err != nil {
		return typeSize{}, false
	}
	return s, true
}

// sizeShrank reports whether the baseline pins name at a size no smaller
// than the current one on both axes. A shrink passes without a baseline
// update; -update lowers the pin.
func sizeShrank(accepted map[string]bool, cur typeSize) bool {
	for l := range accepted {
		if base, ok := parseSizeLine(l); ok && base.name == cur.name {
			return cur.methods <= base.methods && cur.fields <= base.fields
		}
	}
	return false
}

// findUnreferenced returns "pkgpath.Name" (or "pkgpath.Type.Method") for
// every exported object declared in an in-scope package that nothing
// outside that package uses.
func findUnreferenced(all []*packages.Package, inScope map[string]bool) []string {
	used := map[types.Object]bool{}
	var ifaces []*types.Interface
	seenPkg := map[*types.Package]bool{}

	collectIfaces := func(scope *types.Scope) {
		for _, name := range scope.Names() {
			if tn, ok := scope.Lookup(name).(*types.TypeName); ok {
				if it, ok := tn.Type().Underlying().(*types.Interface); ok && it.NumMethods() > 0 {
					ifaces = append(ifaces, it)
				}
			}
		}
	}
	packages.Visit(all, nil, func(p *packages.Package) {
		if p.Types == nil || seenPkg[p.Types] {
			return
		}
		seenPkg[p.Types] = true
		collectIfaces(p.Types.Scope())
		if p.TypesInfo == nil {
			return
		}
		for _, obj := range p.TypesInfo.Uses {
			if obj.Pkg() != nil && obj.Pkg() != p.Types {
				used[obj] = true
			}
		}
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				if it, ok := n.(*ast.InterfaceType); ok {
					if t, ok := p.TypesInfo.TypeOf(it).(*types.Interface); ok && t.NumMethods() > 0 {
						ifaces = append(ifaces, t)
					}
				}
				return true
			})
		}
	})

	implementsAny := func(t types.Type, method string) bool {
		for _, it := range ifaces {
			if it.Method(0) == nil {
				continue
			}
			has := false
			for i := 0; i < it.NumMethods(); i++ {
				if it.Method(i).Name() == method {
					has = true
					break
				}
			}
			if !has {
				continue
			}
			if types.Implements(t, it) || types.Implements(types.NewPointer(t), it) {
				return true
			}
		}
		return false
	}

	var out []string
	for _, p := range all {
		if !inScope[p.PkgPath] || p.Types == nil || p.Name == "main" {
			continue
		}
		generated := generatedFiles(p)
		fromGenerated := func(pos token.Pos) bool {
			return generated[p.Fset.Position(pos).Filename]
		}
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			obj := scope.Lookup(name)
			if !obj.Exported() || fromGenerated(obj.Pos()) {
				continue
			}
			if !used[obj] {
				out = append(out, p.PkgPath+"."+name)
			}
			tn, ok := obj.(*types.TypeName)
			if !ok {
				continue
			}
			named, ok := tn.Type().(*types.Named)
			if !ok {
				continue
			}
			for i := 0; i < named.NumMethods(); i++ {
				m := named.Method(i)
				if !m.Exported() || used[m] || fromGenerated(m.Pos()) {
					continue
				}
				if implementsAny(named, m.Name()) {
					continue
				}
				out = append(out, fmt.Sprintf("%s.%s.%s", p.PkgPath, name, m.Name()))
			}
		}
	}
	return out
}

func generatedFiles(p *packages.Package) map[string]bool {
	generated := map[string]bool{}
	for i, f := range p.Syntax {
		if ast.IsGenerated(f) {
			generated[p.CompiledGoFiles[i]] = true
		}
	}
	return generated
}
