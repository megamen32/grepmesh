# GrepMesh philosophy

GrepMesh optimizes for a comfortable local-first default.

The core rule is simple: prefer obvious defaults over hidden restrictions. If the GrepMesh process can read a configured path, GrepMesh should normally be able to search it. Security policy, narrow allowlists, extra authentication layers, and organization-specific exclusions are opt-in controls rather than invisible behavior changes.

Operational exclusions are different from security policy. GrepMesh skips dependency/build/cache trees and dynamic pseudo-filesystems such as `/proc`, `/sys`, `/dev`, and `/run` because indexing them is noisy, expensive, or unstable. These defaults exist to keep the product fast and predictable, not to decide which user data is sensitive.

The same principle applies to the mesh. The default listener is loopback-only and works naturally behind an existing trusted tunnel. Additional peer authentication is available when a deployment explicitly wants it, but GrepMesh should not force duplicate protection layers onto an already protected transport.

Features with substantial local cost are opt-in, not hidden. Document indexing is local-first. Media transcription is disabled by default because it downloads a model and consumes CPU/GPU time; when enabled, GrepMesh should choose a sensible backend automatically and manage the model cache itself.

In short: make the common path easy, make expensive or granular controls explicit, and avoid surprising denials.
