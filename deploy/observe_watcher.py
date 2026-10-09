#!/usr/bin/env python3
"""Inspect existing GrepMesh watcher coverage without registering watches."""
import json
import os
from pathlib import Path
import resource
import sqlite3
import subprocess
import time

UNIT = "grepmesh-mcp.service"
HOME = Path("/home/roomhacker")
TARGETS = (HOME, HOME / ".grepmesh-canary", HOME / "ServersAdministartion",
           HOME / "agents-projects", Path("/var/lib/grepmesh-mcp/userio-cache"))
DB = Path("/home/roomhacker/.cache/grepmesh/server-100-65415a79c0e91f4b.sqlite3")
MAX_FDINFO_BYTES = 16 * 1024 * 1024
MAX_CHILDREN = 512


def bounded():
    resource.setrlimit(resource.RLIMIT_AS, (96 * 1024**2, 96 * 1024**2))
    resource.setrlimit(resource.RLIMIT_CPU, (8, 8))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1024**2, 1024**2))


def kernel_identity(info):
    # fdinfo uses the kernel dev_t encoding, not libc's st_dev encoding.
    return (os.major(info.st_dev) << 20 | os.minor(info.st_dev), info.st_ino)


def watch_identity(line):
    fields = dict(part.split(":", 1) for part in line.split()[1:])
    return int(fields["sdev"], 16), int(fields["ino"], 16)


def generation():
    names = ("MainPID", "InvocationID", "ControlGroup", "NRestarts",
             "MemoryHigh", "MemoryMax", "MemorySwapMax", "CPUQuotaPerSecUSec",
             "TasksMax", "TasksCurrent", "MemoryCurrent")
    command = ["systemctl", "show", UNIT]
    for name in names:
        command.extend(("-p", name))
    raw = subprocess.check_output(command, text=True, timeout=4)
    result = dict(line.split("=", 1) for line in raw.splitlines())
    pid = int(result["MainPID"])
    if pid <= 0:
        raise RuntimeError("existing service must be active")
    result["start_ticks"] = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    result["cgroup_inode"] = (Path("/sys/fs/cgroup") / result["ControlGroup"].lstrip("/")).stat().st_ino
    return result


def inspect(before):
    start = time.monotonic()
    known = {}
    records = []
    for path in TARGETS:
        info = path.stat()
        item = {"path": str(path), "inode": info.st_ino, "watched": False}
        records.append(item)
        known.setdefault(kernel_identity(info), []).append(item)
    children = []
    child_read_errors = 0
    with os.scandir(HOME) as entries:
        for number, entry in enumerate(entries):
            if number >= MAX_CHILDREN:
                raise RuntimeError("direct home child observation exceeds fixed limit")
            try:
                if not entry.is_dir(follow_symlinks=True):
                    continue
                info = entry.stat(follow_symlinks=True)
                item = {"watched": False, "symlink": entry.is_symlink(),
                        "accessible": os.access(entry.path, os.R_OK | os.X_OK)}
                children.append(item)
                known.setdefault(kernel_identity(info), []).append(item)
            except OSError:
                child_read_errors += 1
    pid = before["MainPID"]
    count = byte_count = 0
    fdinfos = sorted(Path(f"/proc/{pid}/fdinfo").iterdir())
    if len(fdinfos) > 64:
        raise RuntimeError("fd observation exceeds fixed limit")
    for path in fdinfos:
        try:
            with path.open() as stream:
                for line in stream:
                    byte_count += len(line)
                    if byte_count > MAX_FDINFO_BYTES or time.monotonic() - start > 45:
                        raise RuntimeError("fd observation exceeded byte/wall bound")
                    if line.startswith("inotify "):
                        count += 1
                        for item in known.get(watch_identity(line), ()):
                            item["watched"] = True
        except FileNotFoundError:
            continue  # An unrelated transient socket can close during observation.
    # Read-only SQLite, no body scan, queue ACK, marker update or migration.
    db = sqlite3.connect("file:" + str(DB) + "?mode=ro", uri=True, timeout=3)
    db.execute("PRAGMA query_only=ON")
    deadline = time.monotonic() + 4
    db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    row = db.execute("SELECT count(*), sum(CASE WHEN instr(path,'/.tmpbin/')>0 "
                     "OR instr(path,'/forensics_out/')>0 THEN 1 ELSE 0 END) "
                     "FROM grepmesh_pending_ocr").fetchone()
    db.close()
    group = Path("/sys/fs/cgroup") / before["ControlGroup"].lstrip("/")
    events = dict(line.split() for line in (group / "memory.events").read_text().splitlines())
    rust_log = None
    for field in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0"):
        if field.startswith(b"RUST_LOG="):
            rust_log = field[len(b"RUST_LOG="):].decode("utf8", errors="replace")
    return {"watch_count": count, "fdinfo_bytes": byte_count, "coverage": records,
            "home_direct_directory_count": len(children),
            "home_direct_watched_count": sum(c["watched"] for c in children),
            "home_direct_inaccessible_count": sum(not c["accessible"] for c in children),
            "home_direct_symlink_count": sum(c["symlink"] for c in children),
            "home_child_read_errors": child_read_errors,
            "pending": row[0], "metadata_only_pending": row[1] or 0,
            "service_memory_events": events, "rust_log": rust_log,
            "registration_errno": None,
            "registration_error_observed": False,
            "observation_seconds": time.monotonic() - start}


def main():
    bounded()
    before = generation()
    result = inspect(before)
    after = generation()
    identity = ("MainPID", "InvocationID", "start_ticks", "cgroup_inode")
    if any(before[key] != after[key] for key in identity):
        raise RuntimeError("service generation changed; observation is not reusable")
    result.update(kind="grepmesh-watch-observation-v1", observed_unix=time.time(),
                  service=after, same_generation=True, controller_max_rss_kib=resource.getrusage(
                      resource.RUSAGE_SELF).ru_maxrss, controller_cpu_seconds=sum(resource.getrusage(
                      resource.RUSAGE_SELF)[:2]))
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
