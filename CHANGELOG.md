# Changelog

Releases follow [semantic versioning](https://semver.org/). The settings (`SIC_*` environment variables), the metric names and the HTTP API paths are the public interface; a change to them that breaks an existing deployment is a major version.

## Unreleased

- Nodes that miss on the same blob at the same moment now share one registry request. Every digest has a coordinator node, chosen by all nodes alike (rendezvous hashing over the digest and the node addresses); a node that misses asks it first, and the coordinator downloads the blob once and streams it to the others. Before, a rollout sent most nodes upstream, because a peer only shared a download that had already delivered bytes. A coordinator that is down, slow (`SIC_COORDINATE_WAIT`, default `20s`) or not the coordinator in its own view is skipped and the node fetches on its own, as before. The coordinator fetches without credentials, so private content still comes through the node that has them. Needs `HOST_IP` (Kubernetes discovery); `SIC_COORDINATE=false` turns it off. New metrics: `sic_coordinator_hits_total`, `sic_coordinator_served_total`, `sic_coordinator_fallbacks_total`. A mixed-version cluster is safe: an older node ignores the request header and answers as an ordinary peer.
- Saving a new tag no longer blocks tag lookups. The alias file was rewritten and fsynced while holding the lock that every manifest request takes to resolve a tag, so a lookup waited as long as the write took (measured: about 20 ms at 10,000 tags and 240 ms at 100,000). Lookups now wait for nothing; an alias still becomes visible only after it has been saved. `aliases.json` is written compactly instead of indented. Saving one new tag still costs time proportional to the number of tags (about 150 ms at 100,000 in the same measurement); bounding or restructuring the alias store is a separate change.
- New sample `deploy/containerd/mirror-hosts.toml`: SIC as a mirror for one registry, with the registry itself as the fallback, so a node whose SIC pod is down or being upgraded still pulls (and SIC's own image never depends on SIC). It advertises only `pull`, so containerd resolves tags at the registry and a moved tag is not answered from an old cached alias; `resolve` is for registries with upstream fallback enabled. It shows how to trust a private CA for the fallback registry. `deploy/containerd/hosts.toml` (SIC as the only host, `_default`) stays, now documented as the choice for clusters where only the peers can supply images.
- README: a new "Upgrading" section (the DaemonSet cannot surge on a fixed host port; how to seed and pre-pull the new image when SIC is the only host and upstream fallback is off) and a rewritten "Registry mirror" section.
- New `GET /readyz` on the admin and main listeners: it answers `503` with the reason when the cache directory cannot be written (read-only, unmounted, permissions), and `200` otherwise. The sample DaemonSet's readiness probe should use it from this release on (the manifest changes together with its image tag), so a node with a broken cache leaves the other nodes' peer lists within one discovery interval instead of staying in them and failing their requests. A full disk does not make a node unready: it can still serve what it holds. `/healthz` is unchanged and remains the liveness probe.

## 1.0.3 - 2026-10-26

- A cached blob that could not be read (no file descriptors left, an I/O error) was deleted and counted as corrupt when the same blob was stored again. Only a blob that was read in full and does not match its digest is removed now.
- A cached object that cannot be opened for a reason other than being missing is counted in `sic_store_errors_total` and logged, instead of passing as a plain miss.
- A client waiting on a manifest fetch started by another client stops waiting when it goes away, and a manifest request as a whole is bounded to two minutes, so a source that sends a few bytes at a time cannot hold it open.
- Startup fails when the service account's CA file holds no certificate, instead of every pod listing failing later.
- A panic in a download is logged with its stack.
- The cache directory is created with mode 0750, and `aliases.json` and `verified.json` are written with mode 0600 (they were 0755 and 0644). Existing directories keep their mode.
- Built with Go 1.26.10 (was 1.25): the Go 1.25 line has no fix for the standard library advisories GO-2026-6599 to GO-2026-6617, which `govulncheck` reports as reachable.
- `golang.org/x/net` v0.60.0 (with newer `x/sync`, `x/sys`, `x/text`): fixes GO-2026-6603, -6611, -6612 and -6617.
- Known and accepted: `govulncheck` reports GO-2026-5064, -5338, -5622 and -6791 in `github.com/containerd/containerd` v1, which has no fixed release (only containerd/v2 does). All four are about the CRI plugin's checkpoint restore and import, which runs in the containerd daemon. SIC uses only containerd's client library to read the content store and list images, and does not run that plugin. They are listed as accepted in `.github/workflows/ci.yml`, with the reason; moving to containerd/v2 would clear them.
- CI: lint (`.golangci.yml`), race-detector tests, fuzz targets for request and pod-list parsing, and `govulncheck`.

## 1.0.2 - 2026-10-25

- A download that failed without saying how (a panic in the store) left clients that were streaming from it waiting until they gave up; they now get an error at once.
- A damaged `aliases.json` that contains `null` no longer makes the first saved tag panic.
- Pod addresses from the Kubernetes API that are not IP addresses are ignored.
- Docs: the getting-started page no longer tells people to replace a branch image.

## 1.0.1 - 2026-10-25

- An upstream registry's `429 Too Many Requests` is now passed on to the client, with its `Retry-After` when that is a number of seconds or a date, instead of being reported as "not found". A cached tag is still served when the registry throttles the check. Applies to blobs, `HEAD` requests and manifests.
- The public-registries end-to-end test skips a registry that keeps throttling and still runs the others.
- README: the end-to-end section describes the registries mode.

## 1.0.0 - 2026-10-24

First release.

A node-local OCI registry mirror and cache for Kubernetes with containerd, with peer sharing between nodes and an optional upstream fallback.

- Caches blobs and manifests by digest; serves `GET`, `HEAD` and byte ranges; handles containerd's `?ns=` namespaces.
- Peer discovery through the Kubernetes API (Ready pods only), with a short peer timeout and back-off, at most `SIC_PEER_MAX_TRIES` peers per miss, and an immediate re-listing when a node knows no peers.
- Mutual TLS between containerd and SIC and between SIC nodes, with hot-reloaded certificates and a startup audit of the certificate.
- Imports containerd's content and image names through its gRPC API.
- Streams a miss to containerd, and to peers, while it downloads; the last byte is held until the digest has been verified.
- Verifies every blob at write time and in a background scrub; evicts least-recently-used content under a size cap and a free-space reserve.
- Optional upstream fallback for listed registries, relaying registry authentication challenges; tags are confirmed against the registry after `SIC_TAG_TTL`.
- Prometheus metrics, plain-HTTP health and metrics on a loopback admin port.
- Images for `linux/amd64` and `linux/arm64`; `sic --version`.

Known limits: the containerd import keeps a second copy of every blob on disk; a node learns of a newly Ready peer only at the next 30-second listing unless it knew no peers at all; on clusters larger than `SIC_PEER_MAX_TRIES` a blob held by few nodes is often fetched from the registry instead; discovery cost grows with the square of the node count. A peer or registry that declares a very large length for a blob can make a node evict older blobs before the blob is refused; the node never stores it.
