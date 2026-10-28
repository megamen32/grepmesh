#!/usr/bin/env python3
"""Fail-closed paired-cutover guard for the exact remote-index source roots."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

MAX_ENTRIES = 20_000


def quiet_by_metadata(root: str | Path, cooldown_ms: int, *, now_ns: int | None = None) -> bool:
    """Prove every ctime in a bounded tree is older than the cooldown."""
    root = str(root)
    cutoff = (time.time_ns() if now_ns is None else now_ns) - cooldown_ms * 1_000_000
    try:
        root_stat = os.stat(root, follow_symlinks=False)
        if os.path.islink(root) or not os.path.isdir(root) or root_stat.st_ctime_ns >= cutoff:
            return False
    except OSError:
        return False
    pending = [root]
    visited = 0
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    visited += 1
                    if visited > MAX_ENTRIES:
                        return False
                    info = entry.stat(follow_symlinks=False)
                    if info.st_ctime_ns >= cutoff:
                        return False
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(entry.path)
        except OSError:
            return False
    return True


def has_metadata_only_match(root: str | Path, patterns: list[str]) -> bool | None:
    """Check current paths against configured metadata-only globs, bounded and body-free."""
    root = Path(root)
    pending = [root]
    visited = 0
    try:
        root_name = root.name
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    visited += 1
                    if visited > MAX_ENTRIES:
                        return None
                    path = Path(entry.path)
                    relative = path.relative_to(root).as_posix()
                    candidates = [relative, f"{root_name}/{relative}"]
                    is_dir = entry.is_dir(follow_symlinks=False)
                    if is_dir:
                        candidates.extend(
                            (
                                f"{relative}/.grepmesh-directory-probe",
                                f"{root_name}/{relative}/.grepmesh-directory-probe",
                            )
                        )
                    if any(
                        fnmatch.fnmatchcase(candidate, pattern)
                        for candidate in candidates
                        for pattern in patterns
                    ):
                        return True
                    if is_dir:
                        pending.append(path)
    except OSError:
        return None
    return False


def expired_metadata_only_is_stale(
    root: str | Path,
    exact_scope: list[dict[str, Any]],
    config: dict[str, Any],
    now_ms: int,
    cooldown_ms: int,
) -> bool:
    """Accept an expired sticky metadata-only flag only with fresh path evidence."""
    if not exact_scope or any(row.get("hot") is not False for row in exact_scope):
        return False
    last_events = [row.get("last_event_ms") for row in exact_scope]
    if any(not isinstance(value, int) or value <= 0 or now_ms - value < cooldown_ms for value in last_events):
        return False
    index_activity = (config.get("limits") or {}).get("index_activity") or {}
    patterns = index_activity.get("metadata_only_globs") or []
    return has_metadata_only_match(root, patterns) is False


def validate_source_guard(
    phase: str,
    config: dict[str, Any],
    evidence: dict[str, Any] | None = None,
    *,
    now_ms: int | None = None,
) -> None:
    if phase not in {"mcp", "live"}:
        raise ValueError("unknown source guard phase")
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    nodes: dict[str, dict[str, Any]] = {}
    captured: int | None = None
    if phase == "mcp":
        if evidence is None:
            raise ValueError("fresh MCP guard evidence is required")
        captured = evidence.get("captured_at_ms")
        if not isinstance(captured, int) or not 0 <= now_ms - captured <= 60_000:
            raise ValueError("MCP guard evidence must be no more than 60 seconds old")
        status = evidence.get("status") or {}
        if status.get("partial") is not False:
            raise ValueError("MCP status must be complete")
        healthy = {
            row.get("host_id")
            for row in status.get("host_status", [])
            if row.get("ok") is True
        }
        if not {"server-100", "server-44"}.issubset(healthy):
            raise ValueError("both production hosts must be healthy in MCP status")
        nodes = {node.get("host_id"): node for node in status.get("nodes", [])}

    source = config.get("remote_index_source") or {}
    cooldown_ms = int(
        ((config.get("limits") or {}).get("index_activity") or {}).get(
            "hot_cooldown_ms", 3_600_000
        )
    )
    activity = nodes.get("server-100", {}).get("index_directory_activity") or []
    for roots in (source.get("roots") or {}).values():
        for root in roots:
            # A low-volume exact bucket can report hot=false before the
            # configured six-hour ctime quiet period has elapsed. Always prove
            # the exact source tree's cooldown before phase one can mutate 100.
            if not quiet_by_metadata(root, cooldown_ms, now_ns=now_ms * 1_000_000):
                raise ValueError(f"exact source-root ctime cooldown is active or unproven: {root}")
            if phase == "live":
                continue

            matches = [
                row
                for row in activity
                if root == row.get("path")
                or root.startswith(row.get("path", "").rstrip("/") + "/")
                or row.get("path", "").startswith(root.rstrip("/") + "/")
            ]
            if not matches:
                continue
            depth = max(len(row["path"].strip("/").split("/")) for row in matches)
            exact_scope = [
                row for row in matches if len(row["path"].strip("/").split("/")) == depth
            ]
            if all(row.get("hot") is False and row.get("metadata_only") is False for row in exact_scope):
                continue
            scoped_record = any(
                row.get("path") == root
                or row.get("path", "").startswith(root.rstrip("/") + "/")
                for row in exact_scope
            )
            if scoped_record:
                if expired_metadata_only_is_stale(root, exact_scope, config, now_ms, cooldown_ms):
                    continue
                raise ValueError(f"source-root activity bucket is hot or metadata-only: {root}")
            # A hot ancestor may cover unrelated siblings. The ctime walk above
            # independently proves this exact subroot is quiet.

    if phase == "mcp" and captured is not None and now_ms - captured > 60_000:
        raise ValueError("MCP guard evidence expired during source-root check")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("mcp", "live"), required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        evidence = (
            json.loads(args.evidence.read_text(encoding="utf-8"))
            if args.phase == "mcp" and args.evidence
            else None
        )
        validate_source_guard(args.phase, config, evidence)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"[paired-cutover] source-root guard FAIL: {exc}", file=sys.stderr)
        return 1
    print(f"[paired-cutover] source-root guard PASS phase={args.phase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
