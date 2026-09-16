// exportcheck reports exported identifiers in the scoped packages that no
// non-test code outside their own package refers to. Satisfying an
// interface declared anywhere in the module or its imports counts as a
// reference. A committed baseline holds the accepted set; the check fails
// on names not in it.
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
		if !accepted[n] {
			fresh = append(fresh, n)
		}
	}
	if len(fresh) == 0 {
		fmt.Printf("exportcheck: ok (%d accepted)\n", len(unused))
		return
	}
	fmt.Println("exportcheck: exported with no reference outside the package (unexport, or add to the baseline with -update):")
	for _, n := range fresh {
		fmt.Println("  " + n)
	}
	os.Exit(1)
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
		generated := map[string]bool{}
		for i, f := range p.Syntax {
			if ast.IsGenerated(f) {
				generated[p.CompiledGoFiles[i]] = true
			}
		}
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
