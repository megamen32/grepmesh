# Linux release with portable ONNX Runtime

`scripts/build-release.sh` builds the Linux x86_64 release through
`scripts/build-linux-compatible.sh`, then packages its companion `lib/`,
licenses and `onnxruntime-manifest.txt` alongside GrepMesh and ripgrep.
Builds and archives default to `.tmp/`. Deploy the complete archive, keeping
`lib/` next to `grepmesh-mcp`; the executable uses `$ORIGIN/lib` RUNPATH.

The current fleet uses `--no-default-features` (local STT is disabled; OCR
remains available). This preserves that feature selection. The helper pins
Microsoft ONNX Runtime 1.25.1 and verifies the release asset's SHA256:
`eb566a49cfc49ef0642f809b69340b5bb656c7c4905ba873526d226f2c005816`.
Source: https://github.com/microsoft/onnxruntime/releases/tag/v1.25.1.
The manifest also records the bundled shared library checksum; upstream
LICENSE and ThirdPartyNotices.txt accompany it.

The `ort-sys` 2.0.0-rc.13 static download required newer glibc C23 symbols
(`__isoc23_strtol`, `__isoc23_strtoll`, `__isoc23_strtoull`) unavailable on
server-100. The official shared library loads on the existing host without
glibc upgrades or integer-parser wrappers. Its API 17 entrypoint, selected by
the locked dependency features, is checked in the helper.

Run `scripts/build-linux-compatible.sh --test` to build and run the real index
regressions with this shared library and normal Cargo linking. Tests prove
unchanged rebuilds perform no SQLite commits and that edits, deletions and
inconsistent FTS/cache rows are handled. This does not claim OCR inference
quality or model-download availability; no OCR models are downloaded by the
build canary.
