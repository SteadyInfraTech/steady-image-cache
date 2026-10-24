# Changelog

Releases follow [semantic versioning](https://semver.org/). The settings (`SIC_*` environment variables), the metric names and the HTTP API paths are the public interface; a change to them that breaks an existing deployment is a major version.

## Unreleased

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
