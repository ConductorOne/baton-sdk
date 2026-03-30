# Log Rotation

The baton-sdk provides automatic daily log file rotation with gzip compression and configurable retention. This is an SDK-level feature — all connectors built on the SDK inherit it automatically. No changes are required in individual connectors.

## Configuration

Two new CLI flags / config fields are available in every connector:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--log-file` | string | *(empty)* | Path to the log file. When set, enables daily log rotation. |
| `--log-retention-days` | int | `10` | Number of days to keep rotated log files. Range: 1-365. |

### CLI usage

```bash
# Logs to /var/log/baton-http.log with 14-day retention
./baton-http --config-path config.yaml \
  --log-file /var/log/baton-http.log \
  --log-retention-days 14
```

### YAML config

```yaml
log-file: /var/log/baton-http.log
log-retention-days: 14
```

### Behavior when `--log-file` is not set

Logging continues to stderr (the existing default behavior). No files are created, no rotation occurs. This is the zero-change path for existing users.

## How it works

### File naming

Given `--log-file /var/log/baton.log`:

| State | File | Description |
|---|---|---|
| Active | `baton.log` | Current day's log output |
| Rotated | `baton-2026-03-29.log` | Previous day (briefly exists before compression) |
| Compressed | `baton-2026-03-29.log.gz` | Gzip-compressed rotated file |

### Rotation lifecycle

1. **On each write**: The rotator checks if the current date has changed since the last write.
2. **Date boundary crossed**: The active file is closed, renamed with a date suffix (e.g., `baton-2026-03-29.log`), and a fresh active file is opened.
3. **Background compression**: The renamed file is gzip-compressed and the uncompressed version is removed. This happens asynchronously to avoid blocking log writes.
4. **Retention cleanup**: After compression, files older than `--log-retention-days` are deleted. This also runs on startup.
5. **Stale file on startup**: If the process restarts and finds an active log from a previous day, it rotates that file before opening a fresh one.

### Output targets

- **CLI mode** (default): Logs go to both the rotating file AND stderr, so terminal output is preserved.
- **Windows service mode**: Logs go to the file only (no stderr). The file defaults to `%PROGRAMDATA%\ConductorOne\{service-name}\baton.log`.

## Cross-platform support

The rotation implementation works on Linux, macOS, and Windows:

- Uses `filepath` for OS-correct path handling
- Explicitly closes files before deletion (required on Windows where open files cannot be deleted)
- Directory creation uses `os.MkdirAll` with 0750 permissions
- Log files use 0640 permissions (owner read/write, group read)

## Interaction with other logging options

| Option combination | Behavior |
|---|---|
| `--log-file` alone | File + stderr output, daily rotation, default 10-day retention |
| `--log-file` + `--log-retention-days` | File + stderr output, daily rotation, custom retention |
| `--log-format json` + `--log-file` | JSON-formatted output to both file and stderr |
| `--log-format console` + `--log-file` | Console-formatted output to both file and stderr |
| Neither `--log-file` nor `--log-level` changed | Existing behavior, no files, stderr only |

**Note**: If both `--log-file` (via `WithFileRotation`) and `WithOutputPaths` are configured programmatically, `WithFileRotation` takes precedence and `WithOutputPaths` is ignored.

## Thread safety

The `DailyRotator` is fully thread-safe. All writes are serialized through a mutex. Background compression goroutines operate only on already-rotated files and do not contend with active writes.

## Limitations

- **Single-process only**: There is no cross-process file locking. If two processes write to the same log file path, rotation behavior is undefined. Use distinct log file paths per process.
- **Date boundary granularity**: Rotation checks happen on each write. If no writes occur at midnight, rotation is deferred until the next write.
- **Compression is best-effort**: If gzip compression fails (e.g., disk full), the uncompressed rotated file is preserved and will be cleaned up by the retention policy on a subsequent run.

## API for SDK consumers

Connector developers do not need to interact with the rotation API directly. The config fields and CLI flags handle everything. For advanced SDK usage:

```go
import "github.com/conductorone/baton-sdk/pkg/logging"

ctx, err := logging.Init(ctx,
    logging.WithLogFormat(logging.LogFormatJSON),
    logging.WithLogLevel("info"),
    logging.WithFileRotation("/var/log/my-connector.log", 10),
)
```

### New option functions

- `logging.WithFileRotation(filePath string, retentionDays int)` — Enables daily rotation. No-op if `filePath` is empty.
- `logging.WithFileOnly(fileOnly bool)` — When `true`, suppresses stderr mirroring (file output only).
- `logging.DefaultRetentionDays` — The default retention period (10 days), exported as a constant.
