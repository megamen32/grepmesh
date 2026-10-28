#!/usr/bin/env python3
"""Generate an isolated source-only GrepMesh config and exact Git-file manifest.

Writes configuration/state only on explicit invocation. Does not start a
service, edit a client, copy source files, invoke extraction, or change the
existing fleet configuration. Requires native index_activity.source_manifest.
"""
import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

PROJECTS = "/home/roomhacker/ServersAdministartion/ops/xerj/projects.json"
EXTENSIONS = {".rs", ".py", ".go", ".js", ".jsx", ".ts", ".tsx", ".md", ".rst",
              ".toml", ".yaml", ".yml", ".json", ".proto", ".sql", ".sh", ".ps1",
              ".css", ".scss", ".html", ".svelte", ".vue", ".tf", ".h", ".c", ".cpp",
              ".swift", ".kt", ".java", ".xml", ".gradle", ".kts", ".cs", ".fish", ".zsh"}
FILENAMES = {"Dockerfile", "Makefile", "CMakeLists.txt", "LICENSE", "LICENSE.md", "COPYING", "Justfile", "Procfile", ".gitignore", ".dockerignore"}
DENIED_DIRECTORIES = {".git", ".agents", ".codex", ".claude", ".hermes", ".env", "node_modules",
                      "target", "dist", "build", "out", "vendor", "venv", ".venv", "__pycache__",
                      "logs", "log", "backups", "backup", "state", "runtime", "data", "private",
                      "secrets", "credentials", "keys", "cache", ".cache", "tmp", ".tmp",
                      "coverage", ".next", ".nuxt", "generated", "artifacts", "screenshots",
                      "browser-profile", "profiles", "sessions", "downloads", "work"}
DENIED_DIRECTORIES.update({"static", "assets", "payload", "payloads", "evidence", "third_party", "third-party",
                           "thirdparty", "decompiled", "decompile", "compiled", "resources", "fixtures",
                           "certs", "certificates", "var", "generated_code", "generated-code",
                           "dev", "proc", "sys", "run"})
DENIED_NAMES = (".env*", "*.pem", "*.key", "*.p12", "*.pfx", "*.jks", "*.session*", "*.sqlite*",
                "*.db", "*credential*", "*secret*", "*private*", "*password*", "*token*.json",
                "*token*.yaml", "*token*.yml", "auth.json", "cookies*", "*backup*", "*dump*",
                "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "*.min.js", "*.min.css", "*.map")


def eligible(relative, excluded_roots=()):
    parts = Path(relative).parts
    if not parts or Path(relative).is_absolute() or ".." in parts:
        return False
    if any(part.lower() in DENIED_DIRECTORIES for part in parts[:-1]):
        return False
    # Native subtree walking also applies name globs to ancestor directories.
    if any(fnmatch.fnmatch(part.lower(), pattern) for part in parts[:-1] for pattern in DENIED_NAMES):
        return False
    if any(part.startswith(".") for part in parts[:-1]):
        return False
    if any(relative == excluded or relative.startswith(excluded.rstrip("/") + "/") for excluded in excluded_roots):
        return False
    name = parts[-1]
    if name.startswith(".") and name not in {".gitignore", ".dockerignore"}:
        return False
    if any(fnmatch.fnmatch(name.lower(), pattern) for pattern in DENIED_NAMES):
        return False
    if Path(name).suffix.lower() == ".json" and not any(fnmatch.fnmatch(name.lower(), pattern) for pattern in
            ("package.json", "tsconfig*.json", "jsconfig*.json", "plugin.json", "schema.json", "*.schema.json")):
        return False
    if Path(name).suffix.lower() in {".toml", ".yaml", ".yml"} and any(word in name.lower() for word in
            ("credential", "password", "token", "cookie", "session", "secret", "private")):
        return False
    return name in FILENAMES or Path(name).suffix.lower() in EXTENSIONS


def reference_files(manifest_path):
    if not manifest_path.is_absolute() or manifest_path.resolve(strict=True) != manifest_path:
        raise ValueError("reference manifest must be absolute without symlink traversal")
    root = manifest_path.parent
    manifest = json.loads(manifest_path.read_text())
    expected = {"LICENSE.md", "README.md", "httpx/_transports/default.py", "httpx/_config.py", "docs/advanced/transports.md"}
    commit = "b5addb64f0161ff6bfe94c124ef76f6a1fba5254"
    if (manifest.get("version") != 1 or manifest.get("corpus") != "httpx-reference"
            or manifest.get("repository") != "https://github.com/encode/httpx"
            or manifest.get("commit") != commit or manifest.get("license") != "BSD-3-Clause"
            or manifest.get("license_file") != "LICENSE.md"):
        raise ValueError("reference manifest does not identify the selected pinned HTTPX corpus/license")
    entries = manifest.get("files", [])
    if len(entries) != 5 or {entry.get("path") for entry in entries} != expected:
        raise ValueError("reference manifest must contain exactly the five selected HTTPX files")
    admitted = [str(manifest_path)]
    for entry in entries:
        path = root / entry["path"]
        if path.resolve(strict=True) != path or not path.is_file() or path.stat().st_size > 1024 * 1024:
            raise ValueError("reference path is not a bounded regular non-symlink file")
        data = path.read_bytes()
        expected_url = "https://raw.githubusercontent.com/encode/httpx/{}/{}".format(commit, entry["path"])
        if (entry.get("sha256") != hashlib.sha256(data).hexdigest() or entry.get("bytes") != len(data)
                or entry.get("url") != expected_url):
            raise ValueError("reference file fingerprint/provenance mismatch")
        admitted.append(str(path))
    license_text = (root / "LICENSE.md").read_text()
    if "Redistribution and use in source and binary forms" not in license_text or "Neither the name" not in license_text:
        raise ValueError("reference license does not match expected BSD-3-Clause conditions")
    return root, admitted, {"corpus": manifest["corpus"], "repository": manifest["repository"], "commit": commit, "license": "BSD-3-Clause",
                           "manifest": str(manifest_path), "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}


def add_reference(roots, files, references, manifest_path):
    reference_root, reference_paths, provenance = reference_files(manifest_path)
    corpus = provenance["corpus"]
    if corpus in roots:
        raise ValueError("reference corpus name collides with an existing project prefix")
    roots[corpus] = [str(reference_root)]
    references.append(provenance)
    return sorted(set(files).union(reference_paths))


def git_output(root, *arguments):
    result = subprocess.run(["git", "-C", str(root), *arguments], check=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if len(result.stdout) > 32 * 1024 * 1024:
        raise ValueError("Git metadata exceeds bounded manifest budget")
    return result.stdout


def collect(projects):
    files = set()
    roots = {}
    provenance = []
    for project in projects:
        root = Path(project["root"])
        if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
            raise ValueError("project roots must be absolute, existing, and free of symlink traversal")
        prefix = project["prefix"]
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", prefix) or prefix in roots:
            raise ValueError("invalid/duplicate project prefix")
        selected = []
        for encoded in git_output(root, "ls-files", "--cached", "-z", "--", ".").split(b"\0"):
            if not encoded:
                continue
            relative = os.fsdecode(encoded)
            if not eligible(relative, project.get("exclude_roots", [])):
                continue
            path = root / relative
            try:
                if not path.is_file() or path.resolve(strict=True) != path or path.stat().st_size > 1024 * 1024:
                    continue
            except OSError:
                continue
            selected.append(str(path))
        # Empty projects remain discoverable through the explicit root metadata;
        # no untracked fallback or broad parent is admitted.
        roots[prefix] = [str(root)]
        files.update(selected)
        provenance.append({"name": project["name"], "prefix": prefix, "root": str(root),
                           "commit": git_output(root, "rev-parse", "HEAD").decode().strip(),
                           "selected_files": len(selected)})
    if len(files) > 100_000:
        raise ValueError("source manifest exceeds 100000 files")
    return roots, sorted(files), provenance


def atomic_json(path, value, mode):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--projects", type=Path, default=Path(PROJECTS))
    parser.add_argument("--project", action="append", help="select a project prefix; default is the explicit projects manifest")
    parser.add_argument("--state-dir", type=Path, required=True, help="isolated absolute state directory outside every source root")
    parser.add_argument("--bind", default="127.0.0.1:9429")
    parser.add_argument("--host-id", default="ranked-local")
    parser.add_argument("--reference-manifest", type=Path, help="validate and include the five pinned HTTPX public reference files")
    args = parser.parse_args()
    if not re.fullmatch(r"127\.0\.0\.1:[0-9]{1,5}", args.bind) or not 1 <= int(args.bind.rsplit(":", 1)[1]) <= 65535:
        parser.error("bind must be 127.0.0.1 with an explicit valid port")
    projects = json.loads(args.projects.read_text())["projects"]
    if args.project:
        wanted = set(args.project)
        projects = [p for p in projects if p["prefix"] in wanted]
        if {p["prefix"] for p in projects} != wanted:
            parser.error("unknown project prefix")
    if not projects:
        parser.error("no source projects selected")
    roots, files, provenance = collect(projects)
    references = []
    if args.reference_manifest:
        files = add_reference(roots, files, references, args.reference_manifest)
    state = args.state_dir
    if not state.is_absolute() or state.resolve() != state or any(state.is_relative_to(Path(paths[0])) for paths in roots.values()):
        parser.error("state-dir must be absolute, free of symlink traversal and outside source roots")
    if state == Path.home() or state == Path("/"):
        parser.error("state-dir must be a dedicated directory")
    manifest_path = state / "source-manifest.json"
    digest = hashlib.sha256("\n".join(files).encode()).hexdigest()
    manifest = {"version": 1, "files": files, "files_sha256": digest, "projects": provenance, "references": references,
                "refresh": "regenerate and restart to admit new tracked eligible files; edits to admitted paths flow through native watcher"}
    excludes = ["**/" + directory + "/**" for directory in sorted(DENIED_DIRECTORIES)]
    excludes += ["**/" + pattern for pattern in DENIED_NAMES]
    config = {"host_id": args.host_id, "bind": args.bind, "root": projects[0]["root"], "roots": roots,
              "peers": [], "index_enabled": True, "index_path": str(state / "index.sqlite"),
              "search_job_store_root": str(state / "jobs"), "topology_cache_path": str(state / "topology.json"),
              "exclude_globs": excludes, "limits": {"max_file_bytes": 1024 * 1024, "max_results": 30,
                  "max_response_bytes": 131072, "overall_timeout_ms": 10000,
                  "index_activity": {"source_manifest": str(manifest_path)}},
              "stt": {"enabled": False}, "ocr": {"enabled": False}, "compute": {"enabled": False},
              "userio": {"enabled": False}}
    atomic_json(manifest_path, manifest, 0o444)
    atomic_json(state / "config.json", config, 0o600)
    print(json.dumps({"config": str(state / "config.json"), "source_manifest": str(manifest_path),
                      "projects": len(projects), "admitted_files": len(files), "files_sha256": digest}))


if __name__ == "__main__":
    main()
