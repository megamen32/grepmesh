#!/usr/bin/env python3
"""Check memory headroom before paired GrepMesh restarts.

I/O PSI and swap are reported for visibility, but do not block deployment.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


DEFAULT_MIN_MEM_AVAILABLE_KIB = 8 * 1024 * 1024
MAX_MEMORY_FULL_PSI_PCT = 1.0


@dataclass(frozen=True)
class HostMetrics:
    io_full_avg10_pct: float
    io_full_avg60_pct: float
    memory_full_avg10_pct: float
    memory_full_avg60_pct: float
    mem_available_kib: int
    swap_total_kib: int
    swap_free_kib: int


@dataclass(frozen=True)
class ServiceMetrics:
    memory_current_bytes: int
    memory_limit_bytes: int
    memory_full_avg10_pct: float
    memory_full_avg60_pct: float
    swap_current_bytes: int
    oom_kill_events: int


def _pressure_avgs(psi_text: str, label: str) -> tuple[float, float]:
    full_line = next((line for line in psi_text.splitlines() if line.startswith("full ")), None)
    if full_line is None:
        raise ValueError(f"/proc/pressure/{label} has no full row")
    fields = dict(re.findall(r"([a-zA-Z0-9_]+)=([^ ]+)", full_line))
    try:
        avg10 = float(fields["avg10"])
        avg60 = float(fields["avg60"])
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"/proc/pressure/{label} metrics are missing or malformed") from exc
    if not (0 <= avg10 <= 100 and 0 <= avg60 <= 100):
        raise ValueError(f"/proc/pressure/{label} values are outside 0..100 percent")
    return avg10, avg60


def parse_metrics(io_psi_text: str, meminfo_text: str, memory_psi_text: str) -> HostMetrics:
    io_avg10, io_avg60 = _pressure_avgs(io_psi_text, "io")
    memory_avg10, memory_avg60 = _pressure_avgs(memory_psi_text, "memory")
    try:
        mem_available = int(re.search(r"^MemAvailable:\s+(\d+)\s+kB$", meminfo_text, re.M)[1])
        total = int(re.search(r"^SwapTotal:\s+(\d+)\s+kB$", meminfo_text, re.M)[1])
        free = int(re.search(r"^SwapFree:\s+(\d+)\s+kB$", meminfo_text, re.M)[1])
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("memory or swap metrics are missing or malformed") from exc
    if mem_available < 0 or total < 0 or not (0 <= free <= total):
        raise ValueError("memory or swap metrics are invalid")
    return HostMetrics(io_avg10, io_avg60, memory_avg10, memory_avg60, mem_available, total, free)


def violations(
    metrics: HostMetrics,
    service: ServiceMetrics,
    min_mem_available_kib: int = DEFAULT_MIN_MEM_AVAILABLE_KIB,
) -> list[str]:
    reasons = []
    if metrics.mem_available_kib < min_mem_available_kib:
        reasons.append(
            f"MemAvailable {metrics.mem_available_kib / 1024 / 1024:.2f} GiB "
            f"< {min_mem_available_kib / 1024 / 1024:.2f} GiB"
        )
    if service.memory_full_avg10_pct > MAX_MEMORY_FULL_PSI_PCT:
        reasons.append(
            f"GrepMesh cgroup memory PSI full avg10 {service.memory_full_avg10_pct:.2f}% "
            f"> {MAX_MEMORY_FULL_PSI_PCT:.2f}%"
        )
    if service.memory_full_avg60_pct > MAX_MEMORY_FULL_PSI_PCT:
        reasons.append(
            f"GrepMesh cgroup memory PSI full avg60 {service.memory_full_avg60_pct:.2f}% "
            f"> {MAX_MEMORY_FULL_PSI_PCT:.2f}%"
        )
    headroom_bytes = service.memory_limit_bytes - service.memory_current_bytes
    minimum_headroom_bytes = 512 * 1024 * 1024
    if headroom_bytes < minimum_headroom_bytes:
        reasons.append(
            f"GrepMesh cgroup memory headroom {headroom_bytes / 1024 / 1024:.0f} MiB "
            f"< {minimum_headroom_bytes / 1024 / 1024:.0f} MiB"
        )
    return reasons


def _read_cgroup_metrics() -> ServiceMetrics:
    control_group = subprocess.run(
        ["systemctl", "show", "grepmesh-mcp.service", "-p", "ControlGroup", "--value"],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    ).stdout.strip()
    if not control_group.startswith("/") or ".." in Path(control_group).parts:
        raise ValueError("GrepMesh service has no safe systemd cgroup path")
    root = Path("/sys/fs/cgroup") / control_group.lstrip("/")

    def read_int(name: str) -> int:
        value = (root / name).read_text(encoding="ascii").strip()
        if not value.isdigit():
            raise ValueError(f"GrepMesh cgroup {name} is not a finite integer")
        return int(value)

    limits = []
    for name in ("memory.high", "memory.max"):
        value = (root / name).read_text(encoding="ascii").strip()
        if value != "max":
            if not value.isdigit():
                raise ValueError(f"GrepMesh cgroup {name} is malformed")
            limits.append(int(value))
    if not limits:
        raise ValueError("GrepMesh cgroup has no finite memory safety limit")

    events = {}
    for row in (root / "memory.events").read_text(encoding="ascii").splitlines():
        key, value = row.split()
        events[key] = int(value)
    psi10, psi60 = _pressure_avgs((root / "memory.pressure").read_text(encoding="ascii"), "memory")
    return ServiceMetrics(
        memory_current_bytes=read_int("memory.current"),
        memory_limit_bytes=min(limits),
        memory_full_avg10_pct=psi10,
        memory_full_avg60_pct=psi60,
        swap_current_bytes=read_int("memory.swap.current"),
        oom_kill_events=events.get("oom_kill", 0),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    try:
        metrics = parse_metrics(
            Path("/proc/pressure/io").read_text(encoding="ascii"),
            Path("/proc/meminfo").read_text(encoding="ascii"),
            Path("/proc/pressure/memory").read_text(encoding="ascii"),
        )
        service = _read_cgroup_metrics()
    except (OSError, ValueError) as exc:
        print(f"[paired-cutover] resource gate FAIL {args.label}: {exc}", file=sys.stderr)
        return 1
    print(
        f"[paired-cutover] resource {args.label}: "
        f"io_full_avg10={metrics.io_full_avg10_pct:.2f}% "
        f"io_full_avg60={metrics.io_full_avg60_pct:.2f}% (informational) "
        f"mem_full_avg10={metrics.memory_full_avg10_pct:.2f}% "
        f"mem_full_avg60={metrics.memory_full_avg60_pct:.2f}% (host diagnostic) "
        f"grepmesh_cgroup_full_avg10={service.memory_full_avg10_pct:.2f}% "
        f"grepmesh_cgroup_full_avg60={service.memory_full_avg60_pct:.2f}% "
        f"grepmesh_cgroup_memory={service.memory_current_bytes / 1024 / 1024:.0f}MiB/"
        f"{service.memory_limit_bytes / 1024 / 1024:.0f}MiB "
        f"grepmesh_cgroup_swap={service.swap_current_bytes / 1024 / 1024:.0f}MiB "
        f"grepmesh_cgroup_oom_kills={service.oom_kill_events} "
        f"mem_available={metrics.mem_available_kib / 1024 / 1024:.2f}GiB "
        f"swap_free={metrics.swap_free_kib / 1024 / 1024:.2f}GiB"
    )
    reasons = violations(metrics, service)
    if reasons:
        for reason in reasons:
            print(f"[paired-cutover] resource gate FAIL {args.label}: {reason}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
