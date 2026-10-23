package connectorrunner

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// connectorEntryPackages are the packages every connector binary links.
var connectorEntryPackages = []string{
	"./pkg/connectorrunner",
	"./pkg/connectorbuilder",
	"./pkg/cli",
}

// sqliteOnlyDeps must never appear in a connector binary's dependency set.
// pkg/dotc1z/sqlite is the one place v1 (SQLite) c1z files are read, and only
// c1 imports it.
var sqliteOnlyDeps = []string{
	"modernc.org/sqlite",
	"github.com/doug-martin/goqu/v9",
	"github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite",
}

func TestConnectorBinariesDoNotLinkSQLite(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go binary not on PATH")
	}
	root := moduleRoot(t)

	cmd := exec.CommandContext(t.Context(), goBin, append([]string{"list", "-deps", "-f", "{{.ImportPath}}"}, connectorEntryPackages...)...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	deps := make(map[string]struct{})
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			deps[line] = struct{}{}
		}
	}
	require.Contains(t, deps, "github.com/conductorone/baton-sdk/pkg/dotc1z", "sanity: the dependency list must include the store package")
	for _, dep := range sqliteOnlyDeps {
		_, linked := deps[dep]
		require.False(t, linked, "connector packages must not depend on %s", dep)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "go.mod not found above %s", dir)
		dir = parent
	}
}
