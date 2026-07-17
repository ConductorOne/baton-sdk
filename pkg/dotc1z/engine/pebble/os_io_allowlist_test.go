package pebble

// Meta-test: every direct `os.` file-IO call in this package's
// production code must be REGISTERED here with a reason.
//
// Why: the engine performs IO through Engine.fs() (WithVFS) so tests
// can inject faults and run over an in-memory filesystem. The bug
// class this guards against is split-brain IO — a file created
// through one filesystem and read back (or renamed/removed) through
// the other. Concrete instances caught during PR-1 review:
// truncateCheckpointWALs os.ReadDir'ing a checkpoint the DB wrote
// through its FS, and the id-index migration os.Rename'ing an SST it
// created through the engine FS.
//
// The rule of thumb for what belongs on which side:
//   - Anything the pebble.DB ever reads (staged SSTs, the DB dir,
//     checkpoints) MUST go through Engine.fs().
//   - Engine-private scratch that only engine code reads back (spill
//     chunks) and host-side conveniences (MkdirTemp naming, the
//     envelope save in clone-sync) stay on plain os — deliberately,
//     per allowlist entry below.
//
// Adding a new os.* call site: if it can touch anything the DB reads,
// use Engine.fs() instead. Otherwise add a (file, function) entry with
// a reason. Entries that lose their last call site fail the test too,
// so the registry can't rot.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// osFileIOFuncs is the denylist: os package functions that touch the
// filesystem. Non-IO os functions (Getenv, Exit, Stderr, IsNotExist,
// error sentinels, type references) are not tracked.
var osFileIOFuncs = map[string]bool{
	"Create": true, "CreateTemp": true, "Open": true, "OpenFile": true,
	"OpenRoot": true, "ReadDir": true, "ReadFile": true, "WriteFile": true,
	"Truncate": true, "Remove": true, "RemoveAll": true, "Mkdir": true,
	"MkdirAll": true, "MkdirTemp": true, "Rename": true, "Stat": true,
	"Lstat": true, "Link": true, "Symlink": true, "Chmod": true,
	"Chown": true, "Chtimes": true, "TempDir": true,
}

// allowedOSFileIO registers every sanctioned direct os file-IO call:
// file -> os function -> reason. The reason is documentation enforced
// to exist, not parsed.
var allowedOSFileIO = map[string]map[string]string{
	"engine.go": {
		"MkdirTemp": "prepareStagingDir mints the unique host-side staging dir; the engine-FS mirror is created right after via fs.MkdirAll",
		"RemoveAll": "prepareStagingDir failure path + removeStagingDir host-side half; the engine-FS half is removed via fs.RemoveAll",
	},
	"bulk_import.go": {
		"Create": "writeSortedSpillChunk: spill-chunk scratch is engine-private; only readSpillEntry reads it back, never the DB",
		"Open":   "spill-chunk merge readers (engine-private scratch, see Create)",
		"Remove": "failed spill-chunk cleanup (engine-private scratch)",
	},
	"grants.go": {
		"Remove": "ingestSynthLayerSegment deletes merged spill chunks (engine-private scratch)",
	},
	"grant_digest_build.go": {
		"Open": "spill-chunk merge readers (engine-private scratch)",
	},
	"id_index_migration.go": {
		"Open": "spill-chunk merge readers (engine-private scratch)",
	},
	"adapter_clone_sync.go": {
		"Stat":      "clone-sync writes a v3 envelope to a caller-supplied host path; the whole save path is host IO by contract (out of engine-FS scope, like the store layer's envelope save)",
		"MkdirTemp": "clone-sync staging dir for the checkpoint + envelope build (host IO by contract)",
		"RemoveAll": "clone-sync staging dir cleanup (host IO by contract)",
		"OpenFile":  "clone-sync envelope output file (host IO by contract)",
		"Remove":    "clone-sync failed-output cleanup (host IO by contract)",
		"Rename":    "clone-sync atomic tmp->final envelope publish (host IO by contract)",
	},
}

func TestOSFileIOCallsAreAllowlisted(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)

	// found: file -> os func -> positions.
	found := map[string]map[string][]string{}
	for _, pkg := range pkgs {
		for path, f := range pkg.Files {
			base := path[strings.LastIndex(path, "/")+1:]
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				ident, ok := sel.X.(*ast.Ident)
				if !ok || ident.Name != "os" || !osFileIOFuncs[sel.Sel.Name] {
					return true
				}
				if found[base] == nil {
					found[base] = map[string][]string{}
				}
				found[base][sel.Sel.Name] = append(found[base][sel.Sel.Name], fset.Position(call.Pos()).String())
				return true
			})
		}
	}

	// Every found call must be registered.
	var violations []string
	for file, funcs := range found {
		for fn, positions := range funcs {
			if _, ok := allowedOSFileIO[file][fn]; !ok {
				violations = append(violations,
					fmt.Sprintf("os.%s in %s (%s)", fn, file, strings.Join(positions, ", ")))
			}
		}
	}
	sort.Strings(violations)
	require.Emptyf(t, violations,
		"unregistered direct os file-IO in the pebble engine package.\n"+
			"If the file can ever be read by the pebble.DB (staged SSTs, DB dir, checkpoints), use Engine.fs() instead.\n"+
			"If it is genuinely engine-private scratch or host-contract IO, register it in allowedOSFileIO with a reason.\nViolations:\n  %s",
		strings.Join(violations, "\n  "))

	// Every registered entry must still have a call site (no registry rot).
	var stale []string
	for file, funcs := range allowedOSFileIO {
		for fn := range funcs {
			if len(found[file][fn]) == 0 {
				stale = append(stale, fmt.Sprintf("os.%s in %s", fn, file))
			}
		}
	}
	sort.Strings(stale)
	require.Emptyf(t, stale,
		"allowlist entries with no remaining call site — remove them so the registry stays honest:\n  %s",
		strings.Join(stale, "\n  "))
}
