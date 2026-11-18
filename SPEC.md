# Steady Image Cache — Specification

**Status:** Describes the current released product and its intended externally observable behaviour.  
**Audience:** Operators, contributors, reviewers and maintainers.  
**Normative language:** *MUST*, *MUST NOT*, *SHOULD* and *MAY* describe requirements. Where this document describes current implementation limits, those limits are not promises of future capacity.

This specification is a compact statement of what Steady Image Cache (SIC) is for, what it guarantees, and where its guarantees stop. The [README](README.md) remains the operational reference for exact settings, commands, HTTP paths, metrics, examples and deployment instructions. The [changelog](CHANGELOG.md) records changes between releases.

## 1. Purpose

SIC is a node-local, peer-assisted OCI Distribution registry cache for Kubernetes clusters using containerd. It reduces repeated registry transfers by allowing nodes to reuse verified image content already present on neighbouring nodes.

Its primary use case is image delivery in disconnected, air-gapped, bandwidth-constrained or registry-restricted clusters. Upstream registry access is optional and disabled by default. An operator can therefore configure containerd to use SIC as its only registry host and make the absence of cached content an explicit pull failure rather than an implicit bypass to an external registry.

SIC is deliberately a small, Kubernetes-native system: one SIC process per node, normally deployed as a DaemonSet. It has no required central scheduler, manager, database or external storage service.

## 2. Goals

SIC MUST:

1. Present containerd with an OCI-compatible registry endpoint for image manifests and blobs.
2. Reuse content from the local cache before attempting remote sources.
3. Discover eligible SIC peers through Kubernetes and retrieve content from them when available.
4. Verify content-addressed blobs before making them available as completed cache entries.
5. Coalesce concurrent requests for the same missing object within a process.
6. Stream an in-progress blob to waiting clients and peers without requiring the complete blob to arrive first, while withholding successful completion until integrity verification succeeds.
7. Bound cache disk consumption and preserve a configurable free-space reserve.
8. Support mutual TLS for containerd-to-SIC and SIC-to-SIC traffic.
9. Keep registry credentials with the requesting client; SIC MUST NOT become a registry credential store.
10. Expose operational health and Prometheus metrics suitable for monitoring.
11. Run on supported Linux Kubernetes nodes with containerd 1.7 and 2.x, for the versions exercised by the project test suite.

## 3. Non-goals

SIC is not intended to be:

- A general-purpose, independently administered OCI registry or a replacement for a registry's lifecycle, retention or access-management features.
- A piece-level swarm, distributed filesystem or content scheduler for very large artifact fan-out. A blob is transferred as a whole object from a source.
- A guarantee that every peer has every image, or that every pull can be served without contacting an upstream registry.
- A central image pre-puller or rollout orchestrator. The optional `sic preload` command and chart pre-pull mechanism assist operator-controlled rollouts; they do not schedule application deployments.
- A mechanism for bypassing registry authentication or authorizing users. Authorization for protected registry content remains dependent on the registry.
- A guarantee of linear scalability to arbitrary cluster sizes. Peer discovery, peer selection and optional gossip have measurable costs and must be validated at the intended deployment scale.

## 4. Deployment model and components

A normal installation runs one SIC instance on each eligible Kubernetes node.

- **containerd integration:** containerd is configured with a registry `hosts.toml` that directs pulls to the local SIC endpoint. The supplied DaemonSet mounts the containerd socket for import and, where configured, the containerd content directory read-only.
- **HTTP registry interface:** SIC handles OCI manifest and blob requests from containerd. It honours the registry namespace supplied by containerd (including `?ns=`), and supports the relevant HTTP methods and byte-range behaviour described in the README.
- **Local content store:** SIC stores content by digest in its own bounded cache directory. Content may be represented in SIC's cache by a symbolic link to an eligible containerd content file when the content directory is mounted; otherwise it is copied through containerd's gRPC content API. This is a symbolic link, not a hard link.
- **Peer discovery and transfer:** each node discovers Ready, non-terminating SIC pods through the Kubernetes API and requests missing content from eligible peers over the peer HTTP interface.
- **Optional upstream:** when enabled, SIC contacts only explicitly allowlisted registries over HTTPS.
- **Containerd importer:** the optional importer reads containerd through its gRPC API, verifies and imports existing content, and imports image-name-to-digest aliases from the configured containerd namespace.
- **Administration and telemetry:** a separate admin listener exposes health, metrics and the administrative endpoints documented in the README. The Helm chart can add cert-manager integration, monitoring resources and optional large-cluster features.

The project does not require a shared cache volume. Each node owns its local cache; peer transfers are the mechanism for sharing content.

## 5. Pull and cache behaviour

### 5.1 Request handling

For a registry request, SIC MUST preserve the registry namespace and requested object identity. A namespace supplied by a client MUST NOT be treated as an arbitrary upstream hostname.

For a manifest request, SIC resolves the requested tag or digest according to registry semantics. When containerd routes tag resolution through SIC (the supplied air-gapped `hosts.toml` grants SIC the `resolve` capability), SIC first uses a locally stored tag-to-digest alias when it is valid under the configured freshness rules. The containerd importer populates aliases from image names in the configured containerd namespace; in upstream-disabled mode these aliases are not revalidated against a registry and remain usable regardless of age. If no local alias is available, an eligible peer may answer the tag request from its own alias and cached manifest. If neither the local node nor a peer knows the tag, resolution fails: with upstream disabled SIC cannot discover a tag that exists only in a registry. A known alias is useful only while its manifest is available locally or from an eligible peer; alias presence alone does not guarantee that the content has not been evicted. When an allowlisted upstream is enabled, tags are confirmed against the registry according to the configured tag TTL, and the registry's digest—not a peer's opinion of a tag—determines which manifest is fetched. Peers share content by digest. A manifest's digest is calculated from its bytes, and a supplied digest or digest header is checked against that content. Note that the supplied `mirror-hosts.toml` intentionally grants only `pull`: in that mode containerd resolves tags against the registry itself and SIC receives digest-addressed content requests.

For a blob request, SIC uses the requested digest as the content identity. A completed object is served from the local cache when it is present and valid. Otherwise SIC tries eligible peers, followed by the optional upstream source if configured. If no source can provide the object, the request fails; SIC MUST NOT silently contact an unconfigured registry.

### 5.2 Peer requests

A peer request is marked as such. A SIC receiving a peer request serves only content it already has or is already downloading; it MUST NOT forward that request to another peer or to an upstream registry. This prevents request loops.

Peers are tried in a bounded, randomized order by default. A peer that is unavailable, slow to begin responding, or returns unacceptable content is skipped according to the peer timeout and backoff policy. Peer redirects are not followed. Optional EndpointSlice discovery, zone preference and digest gossip can improve peer selection in larger clusters; they are opt-in and remain subject to the scale limitations in the README.

For simultaneous misses, optional coordination chooses a deterministic coordinator for a digest so that nodes can converge on one initial fetch. If coordination cannot be used, a node falls back to its normal peer/upstream path. Coordination is best-effort, not a dependency for correctness. It cannot fetch protected content using another node's credentials.

### 5.3 Concurrent downloads and streaming

Within one SIC process, concurrent requests for the same missing digest MUST share a single in-flight download. The fetch runs independently of the HTTP handler that first requested it: cancellation or disconnect of that client cancels only that client's wait or response stream, not the shared fetch or other clients attached to it. Blob fetches use a cancellation-detached context; individual clients continue to observe their own request contexts. Manifest fetches are similarly detached from an individual client and have a bounded internal fetch timeout.

A blob may be streamed to a client or peer as bytes arrive. Since its digest is not known to be correct until the complete object has arrived, SIC MUST NOT report a successful completed response or publish the completed cache entry until verification passes. The final byte is withheld until that check succeeds. If the source fails or verification fails after response headers have been sent, the response is terminated; clients must treat it as an incomplete transfer and retry.

A request requiring a byte range may wait for the complete object. Cached content supports the HTTP range behaviour documented in the README.

When a source breaks off partway through a blob, SIC may retain the partial bytes for the lifetime of that in-flight download and ask a subsequent source to continue with a range request. The complete assembled object is still verified as a whole. Partial data is not a durable cache entry: it is removed when the shared download finishes, whether successfully or unsuccessfully. Temporary partial files left by an unclean process termination are removed when the store next starts. A client disconnect alone does not end the shared download and therefore does not trigger partial-file cleanup.

### 5.4 Cache integrity and capacity

Every accepted blob MUST be checked against its requested digest. Invalid content MUST NOT be served as a completed cache hit. The cache is also re-checked in the background as described in the README.

Cache use is bounded by the configured size limit and minimum free-space reserve. Eviction is least-recently-used. If a write would violate the reserve or the disk cannot accept it, SIC reports the failure and does not claim to have cached the object. A cache miss caused by eviction is recoverable by fetching the object again from an eligible source.

The cache is an optimization, not the authoritative copy of an image. Operators remain responsible for ensuring that a usable source exists for content that may be evicted or absent.

## 6. Authentication and security boundaries

### 6.1 Transport identity

When TLS is enabled, containerd clients and peer nodes use mutual TLS. A verified client certificate is used to distinguish a node peer from an ordinary registry client; the peer marker header alone is not an identity credential. Certificates must satisfy the names and roles described in the README.

The sample deployment may share a certificate across nodes. Operators requiring per-node identity SHOULD issue per-node certificates and configure the corresponding trust and identity rules.

### 6.2 Registry credentials

SIC does not store registry credentials. It relays registry authentication challenges to containerd and forwards the client's subsequent authorization value to the intended registry origin. Authorization MUST NOT be forwarded across an origin-changing redirect. Registry redirects are followed only over HTTPS and within the documented redirect limit.

Upstream access is disabled by default. When enabled, the destination registry MUST be on `SIC_UPSTREAM_REGISTRIES`; a request namespace MUST never enable arbitrary outbound connections.

With `SIC_COORDINATE_ANONYMOUS_TOKEN` a coordinator that a registry refuses with a Bearer challenge may ask the challenge's token service for a token without any credential and retry once. That token service is an outbound destination like a registry: its host MUST be on `SIC_UPSTREAM_REGISTRIES` too, it is reached over HTTPS only and without following redirects, and no client's credential is sent to it. It is never used for a registry in `SIC_PROTECTED_REGISTRIES`. The token is used only by the coordinator, for content it fetches for others; content an anonymous client may not read is still fetched by the node that holds the credentials.

### 6.3 Protected registries

When `SIC_PROTECTED_REGISTRIES` is configured, SIC treats content from those registries as protected. Before serving protected content from cache, SIC checks with the registry using the requesting client's credentials and the same repository path. A successful check is cached only for the documented short interval. If the registry cannot confirm access, SIC does not serve that protected content.

Protected content may be shared between nodes only under the certificate-based peer identity and protection rules documented in the README. Operators MUST configure the protected registry list consistently on every node. Content cached before a registry is added to the protected list does not automatically acquire the protection mark; follow the README's cache-reset guidance when enabling protection for an existing cache.

SIC does not provide a last-known-good authorization window for protected content while its registry is unavailable. This is an intentional fail-closed boundary.

### 6.4 Kubernetes and host privileges

Peer discovery requires Kubernetes API access sufficient to list and watch the configured peer resources. The containerd importer requires access to the containerd socket and the relevant content/image services. The supplied DaemonSet runs with privileges needed by that integration; operators SHOULD narrow permissions and host access where their containerd socket ownership, ACLs and node configuration allow it.

SIC's cache directory and TLS private keys MUST be writable/readable only by the intended process and principals. The client key used by containerd MUST be distinct from a node identity key where the deployment's trust model requires that separation.

## 7. Containerd import and node bootstrap

The optional importer connects to containerd over its gRPC socket. It MUST NOT modify containerd's content store directly or scrape its private on-disk metadata layout. It imports verified content and image aliases from the configured namespace. A failure for one item SHOULD be recorded without preventing unrelated items from being imported.

When the containerd content directory is mounted read-only, SIC may create a symbolic link in its cache to an eligible regular content file rather than keep a second copy. It does not create a hard link, so cross-filesystem `EXDEV` constraints on hard links do not apply. If the source file is absent, unsuitable, or the symbolic link cannot be created, the importer falls back to reading the content through containerd's gRPC API and copying it into SIC's cache. That fallback still requires the cache directory to be writable and the containerd API to be available. Operators must configure the actual containerd root path for their nodes.

The `sic init-node` command provisions the client certificate files and containerd registry host configuration for the registries selected by the operator. Its optional repeat interval supports certificate renewal. A failed update MUST leave the previous usable files in place.

The optional `sic preload` command pulls named images through the node's SIC, verifies their content and can report peer/upstream byte counts through the admin interface. It does not pin content against eviction; operators SHOULD run it shortly before the rollout that needs the images. Docker credential-file support is limited to the formats described in the README; credential helpers and identity-token stores are not supported.

## 8. Interfaces and compatibility

The following are public, compatibility-sensitive interfaces:

- `SIC_*` environment variables and their defaults.
- HTTP API paths and their externally observable semantics.
- Prometheus metric names and labels.
- Containerd configuration generated or supplied by SIC.
- CLI commands and flags intended for operators.
- Helm chart values that are documented as supported configuration.

These interfaces follow semantic versioning. A change that breaks an existing deployment or changes the meaning of an established interface requires a major version. Additive capabilities and new optional settings may be introduced in a minor version; fixes that preserve the interface may be released as patch versions.

Exact names, defaults, paths, metric definitions, supported chart values and upgrade steps are maintained in the README, chart and changelog. This document intentionally does not duplicate the complete settings or metric catalogue.

## 9. Availability, failures and recovery

SIC is designed to fail explicitly rather than return unverified content or bypass an operator's source policy.

| Condition | Expected behaviour |
|---|---|
| Object is in the valid local cache | Serve it locally. |
| Local miss and a peer has the object | Fetch from the peer, verify, cache and serve. |
| Peer is down, slow or faulty | Skip/back off that peer and try another eligible source. |
| No peer has the object and upstream is disabled | Return a miss/failure; do not contact a registry. |
| Upstream is enabled and allowlisted | Try the configured registry over HTTPS, preserving the client's authentication flow. |
| Upstream requires authentication | Relay the challenge; do not persist the client's credentials. |
| Protected registry access cannot be confirmed | Do not serve protected cached content. |
| Blob is truncated, oversized or has the wrong digest | Refuse completion; discard invalid content and expose the failure through logs/metrics. |
| Cache is full or free-space reserve would be breached | Do not claim the object was cached; report the storage failure. |
| Coordinator is unavailable or unsuitable | Fall back to ordinary peer/upstream fetching. |
| Containerd importer encounters one bad/missing item | Record the item failure and continue importing other eligible items. |

A SIC process restart may lose in-flight downloads and their temporary partial data. Completed cache content remains on persistent node storage. A node that loses its cache can repopulate from containerd import, peers or the explicitly configured upstream registry.

When SIC is configured as containerd's only registry host, upgrading SIC itself requires the new SIC image to be available on each node before the old SIC instance is stopped. Use the documented pre-pull/upgrade procedure; do not assume the new image can be pulled through the old instance after it has terminated.

## 10. Observability

SIC MUST expose the health and metrics endpoints documented in the README and emit structured, actionable logs for local storage failures, peer failures, upstream failures, rejected content, authorization failures and import errors.

Prometheus metrics distinguish, among other things, bytes received from peers and upstream registries, bytes served to clients and peers, verification failures, cache/disk failures, peer/coordinator outcomes, streaming and resumed transfers, and containerd import outcomes. The repository includes alert rules and a Grafana dashboard. Operators SHOULD alert on sustained storage errors, corrupt content, repeated coordinator fallbacks and loss of useful peer availability rather than treating every individual cache miss as an incident.

Health endpoints indicate process/service health; they do not prove that every image needed by a future workload is cached or that an upstream registry is reachable.

## 11. Performance and scale expectations

SIC reduces registry traffic when nodes reuse content already present on peers. It does not guarantee a fixed percentage of traffic saved. The result depends on image overlap, rollout concurrency, peer availability, peer-selection limits, authentication requirements, registry behaviour and network topology.

The current implementation asks peers sequentially and limits the number tried by default. Optional digest gossip and zone-aware ordering can improve selection, but they do not turn SIC into a piece-level swarm. Coordination can reduce simultaneous cold-start pulls in some circumstances, but registries that require a token even for public content may prevent a coordinator without the client's credentials from fetching a new object.

The published pilot measurement is deliberately narrow: it is evidence for one tested scenario, not a cluster-wide capacity claim. Before production use, operators MUST test with representative image sizes, concurrency, node count, registry authentication, network topology and failure conditions. Unit tests cover a client disconnecting while a shared blob download continues and completes, but this is not real-cluster failure or load evidence. In particular, large-cluster behaviour, long-running soak behaviour and abrupt pod/process termination during a download remain areas where the README identifies limited or absent real-cluster evidence.

## 12. Supported platform and release assurance

The project currently builds multi-architecture images for `linux/amd64` and `linux/arm64`. The tested containerd daemon versions and Kubernetes test scenarios are stated in the README and CI workflows; passing those tests is not a claim of compatibility with every Kubernetes distribution or future containerd release.

CI includes Go formatting, vet/lint, race-enabled tests, fuzzing, vulnerability checks, manifest/chart validation, monitoring-rule tests and kind-based end-to-end/operations tests. Release images are built for the supported architectures. From the releases identified in the README, images and Helm charts are signed using keyless cosign identity, and image releases include SBOM and provenance attestations. Operators SHOULD verify the release identity and deploy by digest as shown in the README.

## 13. Acceptance criteria for changes

A change is acceptable when all applicable criteria below are met:

1. **Correctness:** the change preserves OCI/containerd behaviour and the invariants in this specification.
2. **Security:** it does not weaken credential isolation, peer identity, protected-registry checks, outbound registry allowlisting or digest verification.
3. **Bounded resources:** it does not introduce unbounded disk, memory, goroutine, connection or retry growth.
4. **Concurrency:** shared downloads, cache mutation, eviction and shutdown remain safe under the race detector.
5. **Failure handling:** peer, registry, disk, Kubernetes API and containerd failures are explicit, observable and recoverable where possible.
6. **Compatibility:** changes to public settings, metrics, HTTP paths, CLI or chart values follow the compatibility policy and include upgrade guidance.
7. **Tests:** a regression test demonstrates the failure before the fix; integration behaviour is covered by the relevant end-to-end job when unit tests cannot establish it.
8. **Operations:** operator-visible behaviour is documented in the README and recorded in `CHANGELOG.md` when applicable.
9. **Release safety:** applicable CI checks pass, and sample manifests/configuration continue to be exercised as shipped.

## 14. Design principles

The following principles guide future work:

- **Verify before trust:** content identity is its digest; unverified content is never a completed cache entry.
- **Fail closed at trust boundaries:** do not bypass registry allowlists, TLS identity or protected-registry authorization.
- **Credentials belong to the client:** peer sharing must not turn one node's registry credentials into cluster-wide credentials.
- **No hidden fallback:** an operator who disables upstream access must not see SIC reach an upstream registry.
- **Bound every resource:** cache size, free space, retries, peer attempts, request timeouts and temporary data need explicit limits.
- **Local-first, peer-assisted:** a node should satisfy a hit locally and use peers to avoid repeated registry transfers, without requiring a central control plane.
- **Observable failure:** operators need to distinguish a cache miss from an unavailable peer, rejected content, denied registry access or local disk failure.
- **Measure before scaling claims:** benchmarks and pilots must state their workload and limits; modelled costs are not production-scale evidence.

---

## References

- [README and operator reference](README.md)
- [Changelog and compatibility history](CHANGELOG.md)
- [Contributing and CI requirements](CONTRIBUTING.md)
- [Release procedure](RELEASING.md)
- [Security policy](SECURITY.md)
- [Helm chart](charts/steady-image-cache)
