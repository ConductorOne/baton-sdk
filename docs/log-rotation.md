# Log Rotation

The baton-sdk provides automatic log file rotation with gzip compression and
configurable retention. This is an SDK-level feature — all connectors built on
the SDK inherit it automatically. No changes are required in individual
connectors.

Rotation is **size-based** and backed by
[lumberjack](https://github.com/natefinch/lumberjack), the standard Go log
rotation library: the active file is rotated once it exceeds a configurable
size, rotated files are gzip-compressed, and files older than the retention
period are removed.

## Configuration

Four CLI flags / config fields are available in every connector:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--log-file` | string | *(empty)* | Path to the log file. When set, enables size-based log rotation. |
| `--log-max-size-mb` | int | `100` | Size in MB at which the active file is rotated. Range: 1-10240. |
| `--log-max-backups` | int | `10` | Maximum number of rotated files to keep. `0` keeps all within the retention window. Range: 0-10000. |
| `--log-retention-days` | int | `10` | Number of days to keep rotated files. Range: 1-365. |

The defaults (100 MB × 10 backups) bound rotated history to ~1 GB — plus the active file, so ~1.1 GB worst case on disk.

Retention has two independent limits, and a rotated file is removed when it hits **either** one: it is older than `--log-retention-days` (age) **or** beyond the newest `--log-max-backups` (count). Setting `--log-max-backups` to `0` disables the count limit, making retention purely age-based.

### CLI usage

```bash
# Logs to /var/log/baton-http.log, rotating at 200 MB, keeping 14 days
./baton-http --config-path config.yaml \
  --log-file /var/log/baton-http.log \
  --log-max-size-mb 200 \
  --log-retention-days 14
```

### YAML config

```yaml
log-file: /var/log/baton-http.log
log-max-size-mb: 200
log-retention-days: 14
```

### Behavior when `--log-file` is not set

Logging continues to stderr (the existing default behavior). No files are
created, no rotation occurs. This is the zero-change path for existing users.

## How it works

### File naming

Given `--log-file /var/log/baton.log`, lumberjack manages the file set:

| State | File | Description |
|---|---|---|
| Active | `baton.log` | Current log output |
| Rotated | `baton-2026-03-29T15-04-05.000.log` | A previous segment, named with its rotation timestamp (briefly exists before compression) |
| Compressed | `baton-2026-03-29T15-04-05.000.log.gz` | Gzip-compressed rotated file |

### Rotation lifecycle

1. **On write**: lumberjack tracks the active file's size.
2. **Size threshold crossed**: when a write would exceed `--log-max-size-mb`, the
   active file is closed and renamed with a rotation timestamp, and a fresh
   active file is opened.
3. **Background compression**: the rotated file is gzip-compressed and the
   uncompressed copy removed.
4. **Retention**: rotated files older than `--log-retention-days` are removed.

> **Note on time-based rotation.** Rotation is by size, not by calendar day, so
> there is intentionally no timezone/DST handling — a deliberate choice to use
> the standard library rather than reinvent it. If strict one-file-per-day
> behavior is ever required, pair this with `logrotate`/a scheduled trigger
> rather than building date logic into the SDK.

### Output targets

- **With `--log-file`** (default): logs go to both the rotating file AND stderr,
  so terminal output is preserved. Pass `WithFileOnly(true)` to write only the
  file (intended for no-console environments such as a Windows service).
- **Windows service default**: rotation is **not** wired into the service path
  by default yet. Unless `--log-file` is explicitly set, a Windows service still
  uses its existing (unrotated) `baton.log` sink, so that file is not size-bounded.
  Automatically rotating a service's `baton.log` — and which process owns the file
  across the launcher/`_connector-service` split — is pending a design decision
  (see the PR discussion); until then, rotation is opt-in via `--log-file`.

## Interaction with other logging options

| Option combination | Behavior |
|---|---|
| `--log-file` alone | File + stderr output, size-based rotation, defaults of 100 MB × 10 backups (~1 GB) / 10-day retention |
| `--log-file` + `--log-max-size-mb` / `--log-retention-days` | File + stderr output with custom thresholds |
| `--log-format json` + `--log-file` | JSON-formatted output to both file and stderr |
| `--log-format console` + `--log-file` | Console-formatted output to both file and stderr |
| Neither `--log-file` nor `--log-level` changed | Existing behavior, no files, stderr only |

**Note**: If both `--log-file` (via `WithFileRotation`) and `WithOutputPaths`
are configured programmatically, `WithFileRotation` takes precedence and
`WithOutputPaths` is ignored.

## API for SDK consumers

Connector developers do not need to interact with the rotation API directly.
The config fields and CLI flags handle everything. For advanced SDK usage:

```go
import "github.com/conductorone/baton-sdk/pkg/logging"

ctx, err := logging.Init(ctx,
    logging.WithLogFormat(logging.LogFormatJSON),
    logging.WithLogLevel("info"),
    logging.WithFileRotation("/var/log/my-connector.log", logging.DefaultMaxSizeMB, 0, 10),
)
```

### Option functions

- `logging.WithFileRotation(filePath string, maxSizeMB, maxBackups, retentionDays int)` —
  Enables size-based rotation. No-op if `filePath` is empty; non-positive
  `maxSizeMB`/`retentionDays` fall back to the package defaults; `maxBackups <= 0`
  keeps all files within the age limit.
- `logging.WithFileOnly(fileOnly bool)` — When `true`, suppresses stderr
  mirroring (file output only).
- `logging.DefaultMaxSizeMB` — Default rotation size (100 MB), exported as a constant.
- `logging.DefaultMaxBackups` — Default number of rotated files kept (10), exported as a constant.
- `logging.DefaultRetentionDays` — Default retention period (10 days), exported as a constant.
