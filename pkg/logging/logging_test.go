package logging

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSetLogLevelUpdatesActiveLogger(t *testing.T) {
	t.Parallel()

	ctx, err := Init(context.Background(), WithLogLevel("info"))
	require.NoError(t, err, "Init")
	logger := ctxzap.Extract(ctx)
	require.Nil(t, logger.Check(zap.DebugLevel, "debug"), "debug should be disabled at info level")

	require.NoError(t, SetLogLevel("debug"), "SetLogLevel")
	require.NotNil(t, logger.Check(zap.DebugLevel, "debug"), "debug should be enabled after SetLogLevel")
}

func TestSetLogLevelRejectsInvalidLevel(t *testing.T) {
	t.Parallel()

	err := SetLogLevel("verbose")
	require.Error(t, err, "expected invalid log level to fail")
}

func TestWithOutputPathsDedupes(t *testing.T) {
	t.Parallel()

	zc := buildConfig(WithOutputPaths([]string{"stdout", "/tmp/baton.log", "/tmp/baton.log", "stdout"}))
	require.Equal(t, []string{"stdout", "/tmp/baton.log"}, zc.OutputPaths, "repeated output paths should collapse, keeping order")
}

func TestDedupeOutputPathsCollapsesAliases(t *testing.T) {
	t.Parallel()

	// --log-path is a hand-written string slice, so these spellings of one file
	// do occur; each extra entry would otherwise get its own rotator.
	paths := []string{"/var/log/baton.log", "/var/log/./baton.log", "/var/log//baton.log", "stdout", "stdout"}
	require.Equal(t, []string{"/var/log/baton.log", "stdout"}, dedupeOutputPaths(paths))

	// A relative spelling of the same file collapses too.
	cwd, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, []string{"baton.log"}, dedupeOutputPaths([]string{"baton.log", filepath.Join(cwd, "baton.log")}))
}

// zapSinkTestDir is rotatorTestDir for a test whose path zap opens itself (any
// path left unrotated). zap caches sinks in a package-global registry and never
// closes them, so t.TempDir's cleanup cannot remove the file on Windows and
// fails the test; removal here is best-effort instead. The dir cleanup is
// registered first so that, t.Cleanup being LIFO, clearRotators still closes
// any rotator the test did create before the directory is removed.
func zapSinkTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "logging-zapsink-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	clearRotators(t)
	return dir
}

// rotatorTestDir returns a temp dir for a test that creates rotators. Order
// matters: t.TempDir registers its cleanup first so clearRotators' runs before
// it. Reversed, Windows refuses to remove a log file a rotator still holds open.
func rotatorTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	clearRotators(t)
	return dir
}

// clearRotators empties the package-level registry when the test ends, so one
// test's rotators can't be reused - or retired - by the next one.
func clearRotators(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		activeRotatorsMu.Lock()
		defer activeRotatorsMu.Unlock()
		for key, rw := range activeRotators {
			_ = rw.Close()
			delete(activeRotators, key)
		}
		publishActiveRotatorPaths()
	})
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationDedupesFilePaths(t *testing.T) {
	dir := rotatorTestDir(t)
	path := filepath.Join(dir, "baton.log")
	alias := filepath.Join(dir, ".", "..", filepath.Base(dir), "baton.log")

	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{path, "stdout", path, alias}))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	defer activeRotatorsMu.Unlock()
	require.Len(t, activeRotators, 1, "one file must never get two rotators renaming each other's backups")
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationReusesRotatorForSamePath(t *testing.T) {
	path := filepath.Join(rotatorTestDir(t), "baton.log")
	rotation := RotationConfig{MaxSizeMB: 1, MaxBackups: 2}

	_, err := InitWithRotation(context.Background(), rotation, WithOutputPaths([]string{path}))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	previous := activeRotators[canonicalOutputPath(path)]
	activeRotatorsMu.Unlock()
	require.NotNil(t, previous)

	_, err = InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 2, MaxBackups: 7},
		WithOutputPaths([]string{path}))
	require.NoError(t, err, "re-Init")

	activeRotatorsMu.Lock()
	current := activeRotators[canonicalOutputPath(path)]
	activeRotatorsMu.Unlock()

	// Reused, not replaced: the Windows service initializes its logger twice
	// against one path, and the first logger keeps being used afterwards.
	require.Same(t, previous, current, "re-Init must reuse the rotator for a path it still logs to")
	previous.mu.Lock()
	defer previous.mu.Unlock()
	require.NotNil(t, previous.f, "the reused rotator must still be usable")
	require.Equal(t, 7, previous.maxBackups, "the newest configuration should win")
	// Both halves of the config, not just the one that happens to be checked:
	// a reconfigure that silently kept the old size would ship undetected.
	require.Equal(t, rotationBytes(2), previous.maxBytes, "the newest size should win too")
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationIsAtomicUnderConcurrentInits(t *testing.T) {
	path := filepath.Join(rotatorTestDir(t), "baton.log")
	rotation := RotationConfig{MaxSizeMB: 1, MaxBackups: 2}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := InitWithRotation(context.Background(), rotation, WithOutputPaths([]string{path}))
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	activeRotatorsMu.Lock()
	defer activeRotatorsMu.Unlock()
	require.Len(t, activeRotators, 1, "concurrent Inits on one path must leave exactly one rotator")
	rw := activeRotators[canonicalOutputPath(path)]
	require.NotNil(t, rw)
	rw.mu.Lock()
	defer rw.mu.Unlock()
	require.False(t, rw.closed, "the surviving rotator must not have been retired by a concurrent Init")
	require.NotNil(t, rw.f)
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationAdoptAndRetireAreAtomic(t *testing.T) {
	dir := rotatorTestDir(t)
	rotation := RotationConfig{MaxSizeMB: 1, MaxBackups: 2}
	paths := []string{filepath.Join(dir, "a.log"), filepath.Join(dir, "b.log")}

	// Adopt and retire must happen under one hold of the registry lock. Dropping
	// it between them lets each of two concurrent Inits retire what the other
	// just adopted: the registry ends up empty with both rotators closed, and the
	// logger that is actually installed writes to a dead handle. Under a single
	// hold one Init simply wins, and the registry holds its rotators, open.
	//
	// Looped because the interleaving is a race rather than a fixed order; the
	// invariant asserted below holds on every iteration when the section is atomic.
	for round := 0; round < 300; round++ {
		var wg sync.WaitGroup
		for _, p := range paths {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := InitWithRotation(context.Background(), rotation, WithOutputPaths([]string{p}))
				assert.NoError(t, err)
			}()
		}
		wg.Wait()

		activeRotatorsMu.Lock()
		live := make([]*rotatingWriter, 0, len(activeRotators))
		for _, rw := range activeRotators {
			live = append(live, rw)
		}
		activeRotatorsMu.Unlock()

		require.NotEmpty(t, live, "round %d: the winning Init's rotator must stay registered", round)
		for _, rw := range live {
			rw.mu.Lock()
			closed := rw.closed
			rw.mu.Unlock()
			require.False(t, closed, "round %d: a registered rotator must not be retired out from under its logger", round)
		}
	}
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithoutRotationLeavesActiveRotatorsAlone(t *testing.T) {
	// The plain Init below hands its path to zap, which keeps the handle open.
	dir := zapSinkTestDir(t)
	rotated := filepath.Join(dir, "rotated.log")

	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{rotated}))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	rw := activeRotators[canonicalOutputPath(rotated)]
	activeRotatorsMu.Unlock()
	require.NotNil(t, rw)

	// A plain Init used to globally close every rotator, terminally - an
	// API-level behavior change for callers that never opted into rotation.
	_, err = Init(context.Background(), WithOutputPaths([]string{filepath.Join(dir, "plain.log")}))
	require.NoError(t, err, "Init")

	rw.mu.Lock()
	defer rw.mu.Unlock()
	require.False(t, rw.closed, "an Init without rotation must not close an active rotator")
	require.NotNil(t, rw.f)
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationCollapsesSymlinkedPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks on Windows needs elevation")
	}
	dir := rotatorTestDir(t)
	target := filepath.Join(dir, "baton.log")
	link := filepath.Join(dir, "current.log")

	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{target}))
	require.NoError(t, err, "InitWithRotation")
	require.NoError(t, os.Symlink(target, link))

	activeRotatorsMu.Lock()
	previous := activeRotators[canonicalOutputPath(target)]
	activeRotatorsMu.Unlock()
	require.NotNil(t, previous)

	// Two rotators on one file rename and prune each other's backups, so a
	// symlink spelling of a path already being rotated must reuse its writer.
	// Asserting on the identity, not the count: without symlink resolution the
	// second Init both creates a rotator and retires the first, so the map is
	// back to one entry - the wrong one, with the first logger's file closed.
	_, err = InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{link}))
	require.NoError(t, err, "re-Init via symlink")

	activeRotatorsMu.Lock()
	defer activeRotatorsMu.Unlock()
	require.Len(t, activeRotators, 1, "a symlink to a rotated file must not get its own rotator")
	for _, rw := range activeRotators {
		require.Same(t, previous, rw, "a symlink spelling must reuse the writer already open on that file")
	}
}

// TestInitWithRotationSurvivesKeyMissAfterExternalDeletion reproduces the R3
// finding: the *same* configured --log-path, resolved through a symlinked
// directory component, with the log file deleted externally between two
// Inits. canonicalOutputPath's EvalSymlinks fails while the file is absent, so
// the second Init's pre-creation lookup misses the key the first Init's
// rotator is registered under, and a redundant writer gets created. adopting
// the pre-existing rotator instead of overwriting the registry entry with the
// redundant one is what keeps the first rotator from being dropped out of the
// map - unreachable by unregisterUnusedRotators, so never closed - while a
// second live rotator exists on the same file.
// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationSurvivesKeyMissAfterExternalDeletion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks on Windows needs elevation")
	}
	base := rotatorTestDir(t)
	realDir := filepath.Join(base, "real")
	require.NoError(t, os.Mkdir(realDir, 0700))
	linkDir := filepath.Join(base, "link")
	require.NoError(t, os.Symlink(realDir, linkDir))
	path := filepath.Join(linkDir, "baton.log")

	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{path}))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	require.Len(t, activeRotators, 1)
	var original *rotatingWriter
	for _, rw := range activeRotators {
		original = rw
	}
	activeRotatorsMu.Unlock()
	require.NotNil(t, original)

	// Deleted externally, between two Inits using the same configured
	// --log-path: canonicalOutputPath's EvalSymlinks now fails on the next
	// lookup because the file is gone.
	require.NoError(t, os.Remove(filepath.Join(realDir, "baton.log")))

	_, err = InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{path}))
	require.NoError(t, err, "re-Init after external deletion")

	activeRotatorsMu.Lock()
	defer activeRotatorsMu.Unlock()
	require.Len(t, activeRotators, 1, "a missed lookup key must not leave two rotators registered for one path")
	for _, rw := range activeRotators {
		require.Same(t, original, rw, "the pre-existing rotator must be adopted rather than overwritten")
	}
	original.mu.Lock()
	defer original.mu.Unlock()
	require.False(t, original.closed, "the surviving rotator must not have been closed as if it were the redundant one")
}

// Not parallel: asserts on the package-level activeRotators set by Init.
//
// Pins the hazard that batonService.Execute must avoid: a second Init whose
// OutputPaths differ retires the first logger's rotator, and that logger - the
// one the Windows connector keeps using, since runService returns the outer
// context - then fails every write. Execute therefore reuses the logger already
// on its context instead of re-initializing.
func TestInitWithRotationRetiresRotatorTheOldLoggerStillHolds(t *testing.T) {
	dir := rotatorTestDir(t)
	operatorPath := filepath.Join(dir, "operator.log")

	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{operatorPath}))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	rw := activeRotators[canonicalOutputPath(operatorPath)]
	activeRotatorsMu.Unlock()
	require.NotNil(t, rw)

	_, err = InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{filepath.Join(dir, "service-default.log")}))
	require.NoError(t, err, "re-Init with the service default path")

	_, err = rw.Write([]byte("dropped\n"))
	require.ErrorIs(t, err, os.ErrClosed,
		"a re-Init with different OutputPaths silences the previous logger's file")
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationClosesDroppedRotators(t *testing.T) {
	dir := rotatorTestDir(t)
	dropped := filepath.Join(dir, "old.log")
	rotation := RotationConfig{MaxSizeMB: 1, MaxBackups: 2}

	_, err := InitWithRotation(context.Background(), rotation, WithOutputPaths([]string{dropped}))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	previous := activeRotators[canonicalOutputPath(dropped)]
	activeRotatorsMu.Unlock()
	require.NotNil(t, previous)

	_, err = InitWithRotation(context.Background(), rotation, WithOutputPaths([]string{filepath.Join(dir, "new.log")}))
	require.NoError(t, err, "re-Init")

	previous.mu.Lock()
	defer previous.mu.Unlock()
	require.Nil(t, previous.f, "a rotator the new logger no longer uses must be closed, not leaked")
}

// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithoutRotationLeavesOutputPathsToZap(t *testing.T) {
	path := filepath.Join(zapSinkTestDir(t), "baton.log")

	ctx, err := InitWithRotation(context.Background(), RotationConfig{}, WithOutputPaths([]string{path}))
	require.NoError(t, err, "InitWithRotation")
	ctxzap.Extract(ctx).Info("hello")

	activeRotatorsMu.Lock()
	_, tracked := activeRotators[canonicalOutputPath(path)]
	activeRotatorsMu.Unlock()
	require.False(t, tracked, "rotation off must create no rotator")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "hello", "rotation off must leave the path to zap's own file sink")
}

// TestInitWithRotationProtectsNewFileFromConcurrentSiblingPrune reproduces the
// S2 finding: adoptRotators used to open every new file in one loop and only
// register (and publish) them in a second loop afterward. prune() deliberately
// never takes activeRotatorsMu, so a sibling rotator's prune() ran fully
// concurrently with that window - and could delete a brand-new active file
// whose name happened to collide with its own backup pattern, before the
// second loop ever added it to the registry. The window widens with more
// paths adopted in one call (every file is opened before any is registered),
// so each round adopts a batch of new, coincidentally-colliding paths
// alongside the sibling, while a background goroutine hammers the sibling's
// prune() throughout. publishCandidateRotatorPaths closes the window by
// publishing every intended path before any of their files are created.
// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationProtectsNewFileFromConcurrentSiblingPrune(t *testing.T) {
	dir := rotatorTestDir(t)

	// The sibling: an already-registered, live rotator for baton.log. Its
	// prune() claims any file in dir matching "baton.<ts>.log" purely by name
	// coincidence - the same setup as
	// TestRotatingWriter_PruneNeverDeletesAnotherLiveRotatorsActiveFile.
	siblingPath := filepath.Join(dir, "baton.log")
	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{siblingPath}))
	require.NoError(t, err, "InitWithRotation for the sibling rotator")

	activeRotatorsMu.Lock()
	sibling := activeRotators[canonicalOutputPath(siblingPath)]
	activeRotatorsMu.Unlock()
	require.NotNil(t, sibling)

	// Hammer the sibling's prune() for the whole test: -race widens the
	// instrumented window between opening a new file and registering it enough
	// that a tight, uncoordinated loop reliably lands inside it when the fix is
	// absent.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			sibling.mu.Lock()
			_ = sibling.prune()
			sibling.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		close(stop)
		wg.Wait()
	})

	const pathsPerRound = 40
	for round := 0; round < 10; round++ {
		// A batch of fresh names every round, all adopted by one Init call, so
		// the file-opening loop has many opportunities to run ahead of the
		// registration loop - matching the sibling's backup pattern purely by
		// coincidence, exactly like timestamped names a rotation would produce.
		outputPaths := make([]string, 0, pathsPerRound+1)
		outputPaths = append(outputPaths, siblingPath) // reused, not retired as unused
		newPaths := make([]string, 0, pathsPerRound)
		for i := 0; i < pathsPerRound; i++ {
			p := filepath.Join(dir, fmt.Sprintf("baton.20260101T%06d.000Z.log", round*pathsPerRound+i))
			outputPaths = append(outputPaths, p)
			newPaths = append(newPaths, p)
		}

		_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
			WithOutputPaths(outputPaths))
		require.NoError(t, err, "round %d", round)

		for _, p := range newPaths {
			require.FileExists(t, p, "round %d: prune must not delete a new active file before it is registered", round)
		}
	}
}

// Not parallel: replaces the global logger via Init.
func TestInitWithRotationReportsUnusableFilePath(t *testing.T) {
	dir := rotatorTestDir(t)
	unusable := filepath.Join(dir, "not-a-file")
	require.NoError(t, os.Mkdir(unusable, 0700))

	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 1},
		WithOutputPaths([]string{filepath.Join(dir, "baton.log"), unusable}))
	require.ErrorContains(t, err, "failed to create rotating log writer")
	require.ErrorContains(t, err, unusable)
}

// TestInitWithRotationAppliesInitialFieldsToRotatedFile reproduces the F1
// finding: zc.Build() applies InitialFields via zap's Fields option, so they
// live inside the core Build returns, but rotateCore is constructed directly
// from zc and teed on afterwards - without separately applying InitialFields
// to it, enabling rotation would silently strip fields from a log file that
// had them before.
// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationAppliesInitialFieldsToRotatedFile(t *testing.T) {
	path := filepath.Join(rotatorTestDir(t), "baton.log")

	ctx, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{path}),
		WithInitialFields(map[string]interface{}{"tenant_id": "acme"}))
	require.NoError(t, err, "InitWithRotation")

	ctxzap.Extract(ctx).Info("hello")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), `"tenant_id":"acme"`,
		"the rotating core must carry InitialFields the same as the base core")
}

// TestInitWithRotationDoesNotDoubleTeeCollidingPaths reproduces the F2
// finding: dedupeOutputPaths keys on canonicalOutputPath before the files
// exist, where EvalSymlinks fails for both a symlink and its not-yet-existing
// target, so they survive as distinct entries; adoptRotators' post-creation
// registration then finds they resolve to the same rotator and must not tee
// that rotator's core onto the logger twice.
// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationDoesNotDoubleTeeCollidingPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks on Windows needs elevation")
	}
	dir := rotatorTestDir(t)
	target := filepath.Join(dir, "baton.log")
	link := filepath.Join(dir, "current.log")
	require.NoError(t, os.Symlink(target, link))

	ctx, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{link, target}))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	require.Len(t, activeRotators, 1, "one underlying file must get exactly one rotator")
	activeRotatorsMu.Unlock()

	ctxzap.Extract(ctx).Info("hello")

	data, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(data), "hello"),
		"a collision resolving to one rotator must still tee exactly one core onto it")
}

// TestSplitOutputPathsLeavesURLSinksToZap: zap resolves "file://" and any
// scheme registered via RegisterSink. Handing those to the rotator would
// MkdirAll the raw string and create a literal "file:" directory tree, so
// enabling rotation must not change where such an entry logs.
func TestSplitOutputPathsLeavesURLSinksToZap(t *testing.T) {
	kept, files := splitOutputPaths([]string{
		"stdout", "file:///var/log/baton.log", "/var/log/baton.log", `C:\logs\baton.log`,
	})
	require.Contains(t, kept, "file:///var/log/baton.log", "a URL sink belongs to zap, not the rotator")
	require.NotContains(t, files, "file:///var/log/baton.log")
	require.Contains(t, files, "/var/log/baton.log", "plain paths still rotate")
	require.Contains(t, files, `C:\logs\baton.log`, "a drive letter is not a URL scheme")
}
