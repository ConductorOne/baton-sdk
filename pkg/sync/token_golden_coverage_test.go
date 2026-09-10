package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The golden corpus is only as strong as its coverage, and nothing compared
// the token structs against the bytes on disk: #1126 added action_counts with
// no fixture and the suite stayed green. This file is that comparison. Every
// field reachable from serializedTokenV0/V1 must appear, set, in at least one
// fixture the round-trip test asserts, so a new token field arrives with
// bytes pinning its wire name — the one thing a Go round-trip test cannot
// catch, since renaming a tag moves the writer and the reader together.
//
// Coverage is keyed by type and field, not by path: Action is reached through
// both V0's actions and V1's actions_map, and one fixture populating it is
// enough for either route.

// goldenTokenUncoveredFields are fields no fixture sets, with the reason. An
// entry here that turns out to be covered fails too — a stale exclusion hides
// the next gap.
var goldenTokenUncoveredFields = map[string]string{}

// goldenTokenOpaqueTypes end the walk. Their fields are not this package's
// wire surface, so the corpus is not where they are pinned.
var goldenTokenOpaqueTypes = map[string]string{
	"expand.EntitlementGraph": "no writer has serialized the graph into the token since CXE-1376; v1_inline_graph.json holds the shape older SDKs wrote and goldenGraph asserts what it decodes to",
}

// goldenTokenTypes are the roots of the wire surface.
func goldenTokenTypes() []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(serializedTokenV0{}),
		reflect.TypeOf(serializedTokenV1{}),
	}
}

// fieldKey labels a field the way a failure should read: serializedTokenV1.action_counts.
func fieldKey(t reflect.Type, jsonName string) string {
	name := t.Name()
	if name == "" {
		name = t.String()
	}
	return name + "." + jsonName
}

// jsonFieldName is the wire name of a struct field, or "" if encoding/json
// skips the field entirely.
func jsonFieldName(field reflect.StructField) string {
	if !field.IsExported() {
		return ""
	}
	tag := field.Tag.Get("json")
	if tag == "-" {
		return ""
	}
	if name := strings.Split(tag, ",")[0]; name != "" {
		return name
	}
	return field.Name
}

// containerElem unwraps pointers, slices and maps down to the type whose
// fields carry JSON names.
func containerElem(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Map {
		t = t.Elem()
	}
	return t
}

// declaredTokenFields collects every field the token structs can write.
func declaredTokenFields(t reflect.Type, out map[string]string, visiting map[reflect.Type]bool) {
	t = containerElem(t)
	if t.Kind() != reflect.Struct || visiting[t] {
		return
	}
	if _, opaque := goldenTokenOpaqueTypes[t.String()]; opaque {
		return
	}
	visiting[t] = true
	defer delete(visiting, t)

	for i := range t.NumField() {
		field := t.Field(i)
		name := jsonFieldName(field)
		if name == "" {
			continue
		}
		out[fieldKey(t, name)] = field.Type.String()
		declaredTokenFields(field.Type, out, visiting)
	}
}

// coveredTokenFields walks one fixture's decoded JSON alongside the type that
// describes it, recording the fields the bytes actually set. An omitempty
// field that is absent is not covered, which is the point.
func coveredTokenFields(t reflect.Type, value any, out map[string]struct{}) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if _, opaque := goldenTokenOpaqueTypes[t.String()]; opaque {
		return
	}

	switch t.Kind() {
	case reflect.Map:
		entries, ok := value.(map[string]any)
		if !ok {
			return
		}
		for _, entry := range entries {
			coveredTokenFields(t.Elem(), entry, out)
		}
	case reflect.Slice, reflect.Array:
		entries, ok := value.([]any)
		if !ok {
			return
		}
		for _, entry := range entries {
			coveredTokenFields(t.Elem(), entry, out)
		}
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return
		}
		for i := range t.NumField() {
			field := t.Field(i)
			name := jsonFieldName(field)
			if name == "" {
				continue
			}
			present, ok := object[name]
			if !ok {
				continue
			}
			out[fieldKey(t, name)] = struct{}{}
			coveredTokenFields(field.Type, present, out)
		}
	default:
	}
}

// goldenTokenFixtureFiles is every fixture the round-trip test asserts,
// inputs and expected outputs alike.
func goldenTokenFixtureFiles() []string {
	seen := map[string]struct{}{}
	var files []string
	for _, tc := range goldenTokenCases() {
		for _, name := range []string{tc.file, tc.expected} {
			if name == "" {
				continue
			}
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			files = append(files, name)
		}
	}
	sort.Strings(files)
	return files
}

// TestGoldenTokenFieldCoverage is the completeness meta-test for the corpus:
// a token field with no bytes behind it has no pinned wire name.
func TestGoldenTokenFieldCoverage(t *testing.T) {
	declared := map[string]string{}
	for _, tokenType := range goldenTokenTypes() {
		declaredTokenFields(tokenType, declared, map[reflect.Type]bool{})
	}
	require.NotEmpty(t, declared, "the walk found no fields, so it is not walking the token structs")

	covered := map[string]struct{}{}
	for _, name := range goldenTokenFixtureFiles() {
		raw, err := os.ReadFile(filepath.Join(goldenTokenDir, name))
		require.NoError(t, err)
		var decoded any
		require.NoErrorf(t, json.Unmarshal(raw, &decoded), "fixture %s is not JSON", name)
		// Every fixture is read against both roots: V0 and V1 share most
		// wire names deliberately, and a shared key in either shape is a
		// key the corpus pins.
		for _, tokenType := range goldenTokenTypes() {
			coveredTokenFields(tokenType, decoded, covered)
		}
	}

	var uncovered, staleExclusions []string
	for key, goType := range declared {
		_, isCovered := covered[key]
		_, excluded := goldenTokenUncoveredFields[key]
		switch {
		case !isCovered && !excluded:
			uncovered = append(uncovered, key+" ("+goType+")")
		case isCovered && excluded:
			staleExclusions = append(staleExclusions, key)
		}
	}
	sort.Strings(uncovered)
	sort.Strings(staleExclusions)

	require.Emptyf(t, uncovered,
		"no fixture in %s sets these token fields, so nothing pins their wire names. Record a fixture (see the README there) or add the field to goldenTokenUncoveredFields with the reason:\n  %s",
		goldenTokenDir, strings.Join(uncovered, "\n  "))
	require.Emptyf(t, staleExclusions,
		"these fields are listed in goldenTokenUncoveredFields but a fixture does set them; drop the entry:\n  %s",
		strings.Join(staleExclusions, "\n  "))
}

// TestGoldenTokenFixturesAreRegistered catches a fixture that exists on disk
// but no case names: unread bytes assert nothing.
func TestGoldenTokenFixturesAreRegistered(t *testing.T) {
	registered := map[string]struct{}{}
	for _, name := range goldenTokenFixtureFiles() {
		registered[name] = struct{}{}
	}

	entries, err := os.ReadDir(goldenTokenDir)
	require.NoError(t, err)
	var orphans []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		if _, ok := registered[entry.Name()]; !ok {
			orphans = append(orphans, entry.Name())
		}
	}
	sort.Strings(orphans)
	require.Emptyf(t, orphans,
		"these fixtures are in %s but no goldenTokenCases entry reads them:\n  %s",
		goldenTokenDir, strings.Join(orphans, "\n  "))
}
