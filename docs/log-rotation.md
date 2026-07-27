# Log file rotation

By default, `baton-sdk` writes logs to `OutputPaths` (stdout/stderr and/or a
file via `--log-path` or the Windows service's default `baton.log`) as
plain, unrotated files. A long-running connector - especially a Windows
service - can grow that file without bound.

Rotation is an **opt-in** feature: two config fields, off by default, with
no other behavior change.

## Behavior-change note

- **Rotation is OFF by default.** `log-max-size-mb` defaults to `0`, which
  disables rotation entirely; existing deployments that don't set it see no
  change at all.
- **Enabling rotation deletes old rotated files beyond the cap.** Once a log
  file exceeds `log-max-size-mb`, it is renamed aside with a UTC timestamp
  suffix (e.g. `baton.log.20260727T120501.123Z`) and a fresh file is opened.
  Only the newest `log-max-backups` rotated files are kept - older ones are
  deleted automatically. Set `log-max-backups` generously if you rely on
  historical logs for audits; there is no separate archival step.

## Config

```yaml
log-path:
  - /var/log/baton/baton.log
log-max-size-mb: 100    # rotate once the file exceeds 100MB; 0 disables rotation (default)
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
