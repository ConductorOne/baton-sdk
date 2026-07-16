package pebble

// Enforcement for the mutation-path registry (mutation_paths.go): every
// non-test file in this package that touches a raw Pebble write
// primitive must carry a declaration, and every declaration must
// describe a file that still raw-writes. This is how a NEW mutation
// path is forced to state its obligations in the PR that adds it — the
// same mechanism sideEffectAnnotationCoverage uses for side-effecting
// annotations.

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// rawWriteSignals are the textual markers of raw Pebble write usage.
// Deliberately simple substring checks over source text: false
// positives just require a (cheap, documentation-bearing) declaration,
// while a regex clever enough to dodge comments would be a second
// implementation to keep correct.
var rawWriteSignals = []string{
	".NewBatch(",
	".db.Set(",
	".db.Delete(",
	"DeleteRange(",
	".db.Ingest(",
	"IngestAndExcise(",
	".db.Apply(",
}

func TestRawWriteRegistryCoversAllRawWritingFiles(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	detected := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		for _, signal := range rawWriteSignals {
			if strings.Contains(string(src), signal) {
				detected[name] = true
				break
			}
		}
	}

	for _, name := range sortedNames(detected) {
		_, declared := rawWriteRegistry[name]
		require.True(t, declared,
			"%s uses raw Pebble write primitives but has NO mutation-path declaration.\n"+
				"Add an entry to rawWriteRegistry (mutation_paths.go) stating what it writes and how it satisfies\n"+
				"the cross-cutting obligations (secondary indexes, digest invalidation, source-cache manifest,\n"+
				"envelope save, entitlement keyspace note, crash ordering). This is deliberate friction:\n"+
				"every undeclared mutation path in this package's history has shipped the same bug family.",
			name)
	}
	for name := range rawWriteRegistry {
		require.True(t, detected[name],
			"rawWriteRegistry declares %s but it no longer uses raw write primitives (or was renamed); remove or update the stale entry", name)
	}

	// Declarations must actually declare.
	for name, decl := range rawWriteRegistry {
		require.NotEmpty(t, decl.Writes, "registry entry %s: Writes must describe the mutation path", name)
		require.NotEmpty(t, decl.Obligations, "registry entry %s: Obligations must state how (or why not) the cross-cutting obligations apply", name)
	}
}

func TestRawWriteRegistryPinnedByTestsExist(t *testing.T) {
	testFuncs := map[string]bool{}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	testFuncRe := regexp.MustCompile(`(?m)^func (Test\w+)\(`)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		for _, m := range testFuncRe.FindAllStringSubmatch(string(src), -1) {
			testFuncs[m[1]] = true
		}
	}

	for name, decl := range rawWriteRegistry {
		for _, pinned := range decl.PinnedBy {
			require.True(t, testFuncs[pinned],
				"registry entry %s names pinning test %q, which does not exist in this package; fix the name or move the pin into Obligations text (cross-package oracles)",
				name, pinned)
		}
	}
}

func sortedNames(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
