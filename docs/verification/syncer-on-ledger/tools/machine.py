#!/usr/bin/env python3
"""Record non-identifying machine inputs for a cost run; this does not certify idle load."""

import json
import os
import platform
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def command(args):
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    return {"command": args, "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}


cpu = Path("/proc/cpuinfo").read_text()
model = sorted({line.split(":", 1)[1].strip() for line in cpu.splitlines()
                if line.startswith(("model name", "CPU implementer", "CPU part", "Hardware"))})
print(json.dumps({
    "collected_at": datetime.now(timezone.utc).isoformat(),
    "architecture": platform.machine(),
    "kernel": platform.release(),
    "cpu_model_fields": model,
    "logical_cpus": os.cpu_count(),
    "available_cpus": len(os.sched_getaffinity(0)),
    "load_average": os.getloadavg(),
    "cgroup_limits": {name: Path("/sys/fs/cgroup", name).read_text().strip()
                      for name in ("cpu.max", "memory.max", "memory.swap.max")
                      if Path("/sys/fs/cgroup", name).exists()},
    "memory": {line.split(":", 1)[0]: line.split(":", 1)[1].strip()
               for line in Path("/proc/meminfo").read_text().splitlines()
               if line.startswith(("MemTotal:", "MemAvailable:", "SwapTotal:"))},
    "disk": command(["lsblk", "--json", "--output", "NAME,TYPE,SIZE,MODEL,ROTA"]),
    "workspace_filesystem": command(["stat", "-f", "-c", "%T", "."]),
    "artifact_filesystem": command(["stat", "-f", "-c", "%T", tempfile.gettempdir()]),
    "toolchain": command(["env", "GOTOOLCHAIN=go1.26.0", "go", "version"]),
    "classification": "machine snapshot; unloaded-machine qualification requires observations across the run",
}, indent=2, sort_keys=True))
