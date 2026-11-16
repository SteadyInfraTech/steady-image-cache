## Specification & Engineering Guardrails

Before modifying code, manifests, or configuration, read **[SPEC.md](SPEC.md)** in full. `SPEC.md` is the canonical contract for externally observable behavior and system invariants.

### Non-Negotiable Invariants
1. **No Silent Upstream Bypass (SPEC §5.1, §6.2):** Upstream access is disabled by default. When enabled, outbound requests MUST strictly match `SIC_UPSTREAM_REGISTRIES` over HTTPS. Never treat a client-supplied registry namespace (`?ns=`) as an arbitrary upstream host.
2. **Credential & Auth Isolation (SPEC §6.2, §6.3):** Never persist client registry credentials. Never forward `Authorization` headers across origin-changing redirects. For `SIC_PROTECTED_REGISTRIES`, always fail closed if upstream registry access cannot be verified with the requesting client's credentials.
3. **Loop Prevention & Peer Identity (SPEC §5.2, §6.1):** Peer requests MUST only serve locally cached or currently in-flight objects—never forward a peer request to another peer or upstream. When TLS is enabled, verify peer identity via mTLS client certificates, never by HTTP headers alone.
4. **Verify Before Commit (SPEC §5.3, §5.4):** Never mark a cache entry complete or release the final byte of a streamed blob until full digest verification succeeds. Treat dangling symlinks to containerd's content store as cache misses and clean them up cleanly.
5. **Cancellation-Detached Coalescing (SPEC §5.3):** Singleflight/shared downloads MUST run on a cancellation-detached context so one client disconnecting never aborts the shared fetch for other waiting clients. Always clean up temporary partial files on download termination and store startup.
6. **Bounded Resources & Race Safety (SPEC §13):** Bound all caches, buffers, goroutines, retries, and timeouts. All cache mutations, eviction paths, and shutdown sequences MUST pass `go test -race ./...`.

### Workflow & PR Requirements
- **Spec & Docs Sync:** Any change to public `SIC_*` env vars, HTTP paths, metrics, CLI flags, or Helm values MUST update `SPEC.md` (if observable behavior changes), `README.md`, and `CHANGELOG.md`.
- **Regression Tests First:** Bug fixes MUST include a regression test demonstrating the failure mode before the fix.
- **Pre-Push Verification:** Before opening or updating a PR, ensure `gofmt`, `go vet ./...`, and `go test -race ./...` pass cleanly.
