# Bounded external reference corpora

GrepMesh searches configured roots. It does not search all GitHub, infer an
unconfigured repository, or promise the coverage of XERJ Hub. The first selected
reference is HTTPX, pinned to commit
`b5addb64f0161ff6bfe94c124ef76f6a1fba5254` in `encode/httpx`.

`scripts/import_reference_corpus.py` prepares exactly five public files:
`LICENSE.md`, `README.md`, `httpx/_transports/default.py`, `httpx/_config.py`,
and `docs/advanced/transports.md`. It is an explicit import utility, not an
automatic fetch-on-query path. It has not been executed as part of preparing
this document. Missing files, unexpected license text, redirects and requests
over the size budget fail rather than selecting another revision or source.
The importer uses public raw HTTPS without credentials and never executes the
downloaded files. The expected BSD-3-Clause license is checked against the
downloaded pinned LICENSE before publication; until import succeeds this is an
expected license, not an independently completed provenance receipt.

Run with an explicit dedicated reference-state directory outside source
checkouts. This creates selected reference files, not a Git checkout. The
read-only `corpus-manifest.json` records repository, exact commit, license,
source URLs, file sizes and SHA256 fingerprints. Reimport to a new empty
directory for a new commit; do not overwrite a pinned corpus in place.

Expose that directory as the ordinary explicit named root `httpx-reference`,
matching the validated `corpus` field in its provenance manifest. The directory
may retain a commit-specific basename; callers use the stable corpus name.
The generator rejects unknown corpus names and collisions with project prefixes.
With
`limits.index_activity.source_manifest` enabled, admit those exact five paths
and `corpus-manifest.json` in the positive file manifest, then restart the
isolated indexer. No new source copies, broad home root, recursive GitHub
fetch, neural service, OCR or transcript extraction is required. The selected
reference manifest can be read via the existing `read_text` tool; clients must
read it before claiming a source commit or license. Path/root identity alone
is not a provenance receipt. Ranked passage body hashes should match the
local imported file, and its SHA256 should match the corpus manifest.

A controlled acceptance query is “HTTP transport connection pool proxy SSL
configuration”, scoped to that reference root. Verify that the returned
transport/configuration passage matches the pinned file and real line range,
then retrieve the manifest and license. A query for an unconfigured external
repository must remain explicitly outside coverage. Existing XERJ Hub HTTPX
coverage is a separate comparison surface; do not silently proxy to it or
claim its whole corpus was imported by GrepMesh.

## Source-only local corpus policy

`deploy/ranked-local-config.py` reads the existing explicit XERJ projects
manifest, selects current Git-tracked eligible code/document files, and writes
an isolated config plus read-only positive source manifest. It keeps ordinary
project roots, not a root for every file. There are no copied source trees.
Tracked credential/private/state/runtime/log/backup/generated paths and known
credential filenames are denied; source-like extensions and a small set of
standard source filenames form the positive eligibility list. Files over 1MiB,
symlinks and deleted paths are omitted. Unknown/untracked files are never
admitted implicitly.

The native source_manifest seam is required. An exclusion-only snapshot is
insufficient: a new directory or secret file could otherwise be ingested before
configuration regeneration. The immutable manifest gates initial scans,
incremental events, subtree reconciliation, metadata-only writes and OCR
retries before extraction/persistence. Invalid/missing manifests fail closed;
restart prunes persisted entries no longer admitted. New tracked files require
regeneration and restart; edits to already admitted paths use the existing
watcher. Removing a file from the policy requires refreshing the manifest and
restarting before claiming revocation completed.

The tightened selector denies all dot-directory components and dot-files except
`.gitignore` and `.dockerignore`. JSON is limited to package/tsconfig/jsconfig,
plugin and schema filenames. Credential/password/token/cookie/session YAML or
TOML names are denied. Static/assets/evidence, generated code and third-party or
decompiled trees are omitted, which deliberately reduces coverage. A source
authentication module whose name matches the conservative secret/private
denylist may also be absent; coverage is the explicit manifest, never all Git
files. The generator's `--reference-manifest` option validates the exact pinned
HTTPX five-file set, file hashes, upstream URLs, commit and license, then adds
that reference root and its provenance manifest as explicit policy exceptions.

Renaming an admitted source file requires regeneration/restart to admit the
new path. A delete event removes its old cached row; a new path is not inferred
from an old path's permission. Restart revocation prunes only the isolated
configured GrepMesh index/cache and pending OCR entries; it never deletes source
files or changes unrelated indexes. The source manifest is immutable for each
running indexer; regenerating the JSON alone does not change the live policy.
Startup missing/invalid policy is reported as a Degraded index with an explicit
error, rather than an empty Ready corpus.

For native stdio MCP clients use `python3 scripts/grepmesh_stdio.py --endpoint
http://127.0.0.1:PORT/mcp`. An optional `--bearer-env VARIABLE` reads a token
already present in the environment; never place its value in command arguments
or configuration. The bridge mirrors each JSON-RPC method/tool name into the
required HTTP headers, forwards notifications without emitting false responses,
enforces byte/network-time bounds, and performs no retries or redirects.

This is a path-based source policy, not a secret-content classifier. A secret
added inside an already admitted source file remains that file's content;
existing repository secret-handling rules still apply. Do not claim every
tracked source file is safe merely because its suffix is allowed. The positive
manifest governs index ingestion; it is not a replacement for caller filesystem
ACLs or the ordinary exact-mode read policy. Do not add production bearers to
generated configuration or bridge command-line arguments.
