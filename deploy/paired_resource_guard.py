#!/usr/bin/env python3
"""Check memory headroom before paired GrepMesh restarts.

I/O PSI and swap are reported for visibility, but do not block deployment.
"""

from __future__ import annotations

import argparse
import re
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
    min_mem_available_kib: int = DEFAULT_MIN_MEM_AVAILABLE_KIB,
) -> list[str]:
    reasons = []
    if metrics.mem_available_kib < min_mem_available_kib:
        reasons.append(
            f"MemAvailable {metrics.mem_available_kib / 1024 / 1024:.2f} GiB "
            f"< {min_mem_available_kib / 1024 / 1024:.2f} GiB"
        )
    if metrics.memory_full_avg10_pct > MAX_MEMORY_FULL_PSI_PCT:
        reasons.append(
            f"memory PSI full avg10 {metrics.memory_full_avg10_pct:.2f}% "
            f"> {MAX_MEMORY_FULL_PSI_PCT:.2f}%"
        )
    if metrics.memory_full_avg60_pct > MAX_MEMORY_FULL_PSI_PCT:
        reasons.append(
            f"memory PSI full avg60 {metrics.memory_full_avg60_pct:.2f}% "
            f"> {MAX_MEMORY_FULL_PSI_PCT:.2f}%"
        )
    return reasons


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
    except (OSError, ValueError) as exc:
        print(f"[paired-cutover] resource gate FAIL {args.label}: {exc}", file=sys.stderr)
        return 1
    print(
        f"[paired-cutover] resource {args.label}: "
        f"io_full_avg10={metrics.io_full_avg10_pct:.2f}% "
        f"io_full_avg60={metrics.io_full_avg60_pct:.2f}% (informational) "
        f"mem_full_avg10={metrics.memory_full_avg10_pct:.2f}% "
        f"mem_full_avg60={metrics.memory_full_avg60_pct:.2f}% "
        f"mem_available={metrics.mem_available_kib / 1024 / 1024:.2f}GiB "
        f"swap_free={metrics.swap_free_kib / 1024 / 1024:.2f}GiB"
    )
    reasons = violations(metrics)
    if reasons:
        for reason in reasons:
            print(f"[paired-cutover] resource gate FAIL {args.label}: {reason}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
