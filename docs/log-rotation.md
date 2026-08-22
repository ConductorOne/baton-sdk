# Log file rotation

By default, `baton-sdk` writes logs to `OutputPaths` (stdout/stderr and/or a
file via `--log-path` or the Windows service's default `baton.log`) as
plain, unrotated files. A long-running connector - especially a Windows
service - can grow that file without bound.

Rotation is an **opt-in** feature: two config fields, off by default, with
no other behavior change.

## Behavior-change note

- **Rotation is OFF by default.** `log-max-size-mb` defaults to `0`, which
  disables rotation entirely; `--log-path` entries are left to zap's own file
  sink exactly as before. Two changes do apply unconditionally:
  - Repeated `--log-path` entries naming the same file (`/var/log/baton.log`
    and `/var/log/./baton.log`, or `C:\logs` and `C:\Logs`) now collapse to
    one entry instead of double-logging every line.
  - **Windows service only:** the service used to re-initialize its logger on
    start, pinning its own output to console format at `info` regardless of
    configuration. That second initialization is gone, so `baton.log` and
    `zap.L()` now honor `--log-format` and `--log-level` - which for a service
    means JSON by default, where it was console before.
- **Enabling rotation deletes old rotated files beyond the cap.** Once a log
  file exceeds `log-max-size-mb`, it is renamed aside with a UTC timestamp
  inserted before its extension (e.g. `baton.20260727T120501.123Z.log`) and a
  fresh file is opened. Only the newest `log-max-backups` rotated files are
  kept - older ones are deleted automatically. Set `log-max-backups`
  generously if you rely on historical logs for audits; there is no separate
  archival step.
- **`log-max-backups: 0` keeps no history at all.** It is not "unlimited": the
  file is renamed aside and then immediately deleted on every rotation, so at
  any moment you only have the log lines written since the last rotation.
  Nothing older survives a crash investigation. Use it only when the log file
  exists purely to bound disk usage.
- **File permissions are unchanged.** Rotated and reopened files use the same
  `0666` mode zap itself uses, so turning rotation on doesn't change who can read
  your logs. On Windows the mode is all but ignored and ACLs govern - restrict the
  log directory itself, with or without rotation.
- **Backups from earlier builds are still cleaned up.** A pre-release build of
  this feature named backups `baton.log.<timestamp>` rather than
  `baton.<timestamp>.log`, and disambiguated same-millisecond rotations with a
  `.<n>` suffix rather than today's `_<nn>`. All of those layouts are recognized
  and pruned, so upgrading doesn't strand the old files.
- **A log file whose rotation keeps failing is bounded, not unbounded.** See
  [Diagnostics](#diagnostics): past 10x `log-max-size-mb` the writer starts
  dropping lines rather than filling the volume.

## Config

```yaml
log-path:
  - /var/log/baton/baton.log
log-max-size-mb: 100    # whole integer MB; rotate once the file exceeds 100MB. 0 disables (default), minimum 1 (= 1MB)
log-max-backups: 5      # keep the 5 newest rotated files; 0 keeps none
```

## CLI equivalents

```bash
./baton-connector \
  --log-path=/var/log/baton/baton.log \
  --log-max-size-mb=100 \
  --log-max-backups=5
```

On Windows, when running as an installed service without `--log-event-log`,
rotation applies to the service's default log file
(`%PROGRAMDATA%\ConductorOne\<name>\baton.log`) the same way - just set
`log-max-size-mb`/`log-max-backups` in the service's `config.yaml`.

## What this is not

This is intentionally minimal:

- No compression of rotated files.
- No age-based retention (`--log-retention-days`) - only size + count.
- No dependency on `lumberjack` or any other third-party rotator.
- No `zap.RegisterSink` usage - rotation is composed as an extra
  `zapcore.Core` teed onto the base logger (the same mechanism used for the
  Windows event log core), so drive-letter/URL sink parsing quirks never
  come into play.

If you need compression or time-based retention, run rotation outside the
process (e.g. `logrotate` on Linux) instead of layering it on top of this.

## Single-writer caveat

In the parent-service + `_connector-service` child-process model, only
**one** process should hold rotation settings for a given log file. Rotation
renames the active file out from under any file descriptor that's still
writing to the old path; if two processes both think they own rotation for
the same path, they will race on the rename and each may lose or duplicate
log lines around the rotation boundary. Configure `log-max-size-mb` /
`log-max-backups` for the single process that owns the file (typically the
parent/service process), not for every child.

Within a single process this is mostly handled for you: a log path gets exactly
one rotating writer, reused if the logger is re-initialized. Paths are matched
after cleaning, making them absolute (relative to the process's working
directory as of its first use, cached for the process's lifetime so a later
`chdir` can't split one configured relative path into two keys), resolving
symlinks when the file already exists, and case-folding on Windows. This is a
strong default, not a guarantee of file identity - the following can still slip
past it and give one file two rotators, which will rename and prune each
other's backups:

- A symlink created, or a directory it passes through re-pointed, after the
  first writer already opened the file - the registration key was resolved
  before that symlink existed.
- A Windows 8.3 short name (`C:\PROGRA~1\...`), a hard link, or a UNC path
  spelled two ways.
- Prune's ownership check is also purely in-process: it will not delete a path
  it can see is another rotator's active file *in this process*, but a
  rotator in a *different* process (or a second `baton-sdk` binary) writing a
  file that happens to match this writer's backup name pattern is invisible to
  it and can still be deleted. This is the one process = one rotation owner
  rule above, restated as a pruning hazard rather than a rename race.
- On a case-insensitive but non-Windows filesystem (macOS APFS in its default
  mode, a case-insensitive Linux mount), two spellings of one path that differ
  only in case resolve to the same file on disk but produce two different
  registry keys - case-folding here is gated on `runtime.GOOS == "windows"`.
  Two rotators end up on one file exactly as above.
- **On Windows, any other open handle on the file blocks rotation outright.**
  Go's `os.OpenFile` does not request `FILE_SHARE_DELETE`, so while any other
  process - or a handle it inherited, such as a child process's stderr wired
  to the same path - has the log file open, `os.Rename` fails with a sharing
  violation and rotation cannot succeed. This does not affect the ordinary
  single-writer case: `rotate()` closes its own handle before renaming. When
  rotation is persistently blocked this way, the writer degrades to the
  documented oversize ceiling (bounded file, repeated diagnostics - see
  [Diagnostics](#diagnostics)) rather than growing without bound.

Spell the log path the same way everywhere, and keep it on a single volume
owned by a single process.

## Diagnostics

A rotation that fails - a rename blocked by another open handle, a full or
read-only log directory, a backup that can't be deleted - is reported and
then ignored: the line is appended to the oversized active file rather than
dropped, and rotation is retried on a short timer. Transient faults (an
antivirus scanner briefly holding the file open) therefore cost nothing but a
temporarily oversized log.

**A permanent fault is bounded.** Appending forever would eventually fill the
volume, which loses every subsequent line *and* takes the service down with it -
strictly worse than losing lines. So once the active file passes **10x**
`log-max-size-mb`, further lines are dropped instead of appended, and the loss
is reported on the same retry cadence as the rotation failure itself. The
outcome of a permanent fault is a file capped at ten times the configured size
(plus at most one line, so a single log line too large to fit under the ceiling
is never dropped forever) and a diagnostic that repeats for as long as the fault
lasts - the fault is never silent, and the file is never unbounded.

One caveat on the accounting, as opposed to the fault: drops are counted between
diagnostics, and that partial count is only flushed when rotation recovers or
when the log file is closed - which happens on a re-`Init` that stops logging to
that path. Raising `log-max-size-mb` on a running logger stops the dropping but
does not report the count, and nothing closes the log file at process exit, so a
count accumulated inside the final retry window - at most one window's worth, not
an outage's worth - is lost if the process exits while the fault is still active.
The fault itself will already have been reported by the preceding diagnostic.

The 10x headroom is deliberately generous:
real transient faults clear well inside it, so only a genuinely stuck rotation
reaches the ceiling. Rotation is retried throughout, so the first successful
rotation ends both the oversize and the dropping.

These diagnostics cannot
go through the logger itself (rotation runs inside a log write), so they are
written to stderr, or to the Windows event log when running as a service.
**Limitation:** a Windows service started without `--log-event-log` writes
them to an event log source that may not be registered under the connector's
name, in which case they appear in the Application log with a "description
not found" wrapper. If the diagnostics matter to you, run with
`--log-event-log`.
