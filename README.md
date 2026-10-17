# Steady Image Cache (SIC) v3

A node-local, peer-assisted OCI registry cache for Kubernetes/containerd, designed for disconnected and air-gapped clusters.

## v3 highlights

* OCI manifest and blob proxy/cache with containerd `?ns=` registry namespace handling.
* Kubernetes-native peer discovery (DaemonSet peers discovered through the Kubernetes API).
* mTLS for containerd→SIC and SIC→SIC traffic.
* Prometheus-format `/metrics` endpoint.
* **Direct containerd gRPC integration** through `/run/containerd/containerd.sock`; no `ctr` subprocess and no read-only scraping of containerd's on-disk layout. SIC walks the containerd content store, verifies/imports content by digest, and imports image-name → target-digest aliases from the `k8s.io` namespace.
* **Single-flight downloads**: concurrent requests for the same missing blob or manifest are coalesced into one peer/upstream fetch per SIC process.
* **HTTP byte-range support** for cached blobs/manifests via Go `http.ServeContent`, including `GET`, `HEAD`, `Range`, `Accept-Ranges`, and 416 handling.
* Upstream fallback remains optional and is disabled by default. When enabled it only contacts registries listed in `SIC_UPSTREAM_REGISTRIES`.

## Pull flow

```text
containerd -> local SIC -> local cache
                         -> peer SIC(s) -> peer cache/containerd-imported content
                         -> optional upstream
```

On startup and periodically, SIC connects to the node's containerd socket. Existing content is copied into SIC's content-addressed cache and image references are imported as aliases. This means a newly joined air-gapped node can obtain content that existed on older nodes before SIC was deployed.

## Containerd integration

Mount the socket into SIC:

```yaml
- name: containerd-socket
  hostPath:
    path: /run/containerd/containerd.sock
    type: Socket
```

Relevant environment variables:

```text
SIC_CONTAINERD_IMPORT=true
SIC_CONTAINERD_ADDRESS=/run/containerd/containerd.sock
SIC_CONTAINERD_NAMESPACE=k8s.io
SIC_IMPORT_INTERVAL=5m
```

A blob or image name that cannot be imported does not stop the import: the rest is still imported, and the failures are logged at the end of each run (the first few, with a count) and counted in `sic_containerd_import_errors_total`. Blobs that containerd's garbage collector deletes while the import runs are ignored. If the cache has no room left (see `SIC_MIN_FREE_BYTES`) the copying stops for that run, the image names are still imported, and the run reports the full cache.

The containerd account/socket permissions must allow the SIC process to call the content and image services. The supplied DaemonSet runs the container as UID 0 for that reason; tighten this if your containerd socket group/ACL permits it.

## Peer requests

Requests that SIC sends to its peers carry an `X-SIC-Peer` header. A SIC that receives one answers from its own cache only and never forwards it to other peers or upstream, so two nodes that both miss cannot ask each other in a loop.

A peer that does not accept a connection and start answering within `SIC_PEER_TIMEOUT` (default `5s`) is skipped for `SIC_PEER_BACKOFF` (default `30s`; `0` disables skipping), so a dead node slows one request instead of every miss. Peers discovered through Kubernetes must be Ready and not terminating.

## Streaming a miss

A layer that is not cached is passed on to containerd while it is still being downloaded, so the pull starts at once instead of waiting for the whole layer to arrive first. Everyone asking for the same blob shares one download, which goes on if a client leaves, and every one of them is fed from the file as it is written.

The digest can only be checked at the end, so the last byte of a response is held back until the check has passed. If it fails, or the source breaks off, the response stops one byte short and the connection is closed: containerd sees an error and retries, and the bad content is never stored. The earlier bytes were sent unverified; containerd checks every layer against its digest itself. A `HEAD` request or a request with a `Range` header needs the whole blob and waits for it. `sic_streamed_responses_total` counts streamed responses and `sic_stream_aborts_total` those that were cut short.

When a source fails after the status line has been sent, the client gets a broken response and not the `404` it would have got before; a peer that fails halfway through is replaced by the next one for later clients.

## Download limits

A digest can only be checked once a download has finished, so SIC bounds what it will accept before then:

* A blob from a peer or registry must declare its `Content-Length`; that length is the most SIC will write, and it must fit on the disk.
* A manifest may be at most 4 MiB. Its digest is computed from its content; a `Docker-Content-Digest` header is only cross-checked, never required.
* `SIC_MIN_FREE_BYTES` (default 1 GiB, `0` for none) and `SIC_MIN_FREE_PERCENT` (default 20, `0` for none) are the free disk space that has to remain after a download; the larger of the two applies. When a download would break that, SIC first makes room (see below) and only then refuses it.
* Responses are never decompressed on the way in, so the stored bytes are exactly what the digest covers.

## Integrity

A blob is hashed while it is written, so it is known to be good when it lands in the cache. SIC remembers that (the blob's inode, size and modification time, in `verified.json` in the cache directory, saved every 30 seconds and on shutdown), and serves such a blob without hashing it again, also after a restart. A blob whose file changed, or that SIC has no record of, is hashed the first time it is opened; if it does not match its digest it is deleted and fetched again.

Hashing on the request path made the first read of every blob wait for a full pass over the file, and made every restart re-read the whole cache. To still find blobs that were damaged on disk without their metadata changing, a background scrubber checks every blob against its digest, slowly, and deletes the ones that do not match (logged, and counted in `sic_corrupt_blobs_total`):

* `SIC_SCRUB_INTERVAL` (default `24h`, `0` turns it off) is the time between complete passes. The time of the last pass is saved, so restarts do not start a new one; a store that was never scrubbed waits ten minutes after start-up.
* `SIC_SCRUB_RATE` (default 32 MiB/s, in bytes per second) limits how fast it reads.

Whatever consumes a blob verifies it as well: containerd checks layer digests, and a peer hashes what it receives.

## Cache size and eviction

The cache no longer grows until the disk is full. When a download would leave less free space than the reserve above, or would take the cache over `SIC_MAX_CACHE_BYTES` (default `0`: no limit), SIC removes the least recently used blobs until it fits, a little more than needed so that the next downloads do not each have to scan the cache. Only if even removing everything would not be enough does the download fail with `507`; SIC does not empty the cache in vain.

* The default reserve of 20% of the filesystem keeps SIC clear of the kubelet's own thresholds: with the usual defaults it starts evicting pods and garbage-collecting images when the image filesystem is less than 15% free. SIC's cache directory is normally on the same filesystem as containerd's. Lower `SIC_MIN_FREE_PERCENT` only if you know the node's eviction thresholds.
* "Used" means stored or served. A blob stored or served in the last minute is never evicted, and neither is a blob that is being read or written at that moment. The time is kept in `verified.json`, not in the file's modification time, so it survives a restart.
* The containerd import never evicts: it copies content that containerd already holds, so when the cache is full it stops for that run instead of pushing out blobs fetched for a pull. It also does not copy back what was evicted.
* Exposed as `sic_cache_bytes`, `sic_evicted_blobs_total` and `sic_evicted_bytes_total`.

An evicted tag still resolves; the manifest is fetched again from a peer when somebody asks. Image names and their digests in `aliases.json` are not removed.

## Upstream fallback

Set `SIC_UPSTREAM_ENABLED=true` and list the registries SIC may contact in `SIC_UPSTREAM_REGISTRIES` (comma separated, e.g. `docker.io,ghcr.io`). The registry is taken from the `?ns=` parameter of the request; a registry that is not listed is refused. Upstream connections are verified against the system CA roots (the peer CA is only used between SIC nodes), so the image needs a CA bundle in that case. The `scratch` base image has none.

SIC does not relay an authentication challenge to the client. A registry that answers `401` (most private registries, and Docker Hub for anonymous pulls) therefore cannot be used for fallback; the request ends as a 404 and the `401` is logged. Use it for registries that serve images without a token.

## Failures

When SIC cannot cache something it says why, instead of answering 404 as if the image did not exist:

* `507` when the free disk space would drop below `SIC_MIN_FREE_BYTES` (or the disk is full); counted in `sic_disk_full_total`.
* `500` when the cache directory cannot be written; counted in `sic_store_errors_total`.
* `400` for a repository name that is not valid per the OCI distribution spec. This is the caller's mistake and does not take any peer out of rotation.

Both conditions, and non-404 answers from peers and upstream registries, are logged at most once per minute per cause, with a count of the lines held back.

## Health and metrics

With mTLS enabled the main listener requires a client certificate on every path, including `/healthz`, which kubelet cannot present. Set `SIC_ADMIN_LISTEN` (the sample DaemonSet uses `127.0.0.1:5001`) to serve `/healthz` and `/metrics` over plain HTTP on that address as well.

## Registry mirror

See `deploy/containerd/hosts.toml`. Install it as the `_default/hosts.toml` registry configuration on each node, adjusting certificate paths for your environment.

## TLS

The sample DaemonSet expects a `steady-image-cache-tls` Secret containing:

* `tls.crt`
* `tls.key`
* `ca.crt`

Peer certificates should contain the DNS SAN configured by `SIC_TLS_SERVER_NAME` (default `steady-image-cache`). Peers are discovered by IP, but TLS verifies this stable server name.

SIC refuses to start unless mutual TLS is fully configured: `SIC_TLS_CERT`, `SIC_TLS_KEY` and `SIC_TLS_CA` are all required (`SIC_TLS_CLIENT_CERT` and `SIC_TLS_CLIENT_KEY` default to the server certificate). Without them it would either serve plain HTTP on every interface or accept any client. For local development only, `SIC_INSECURE=true` turns TLS and authentication off completely and logs a warning.

The certificate, key and CA files are re-checked every 10 seconds and reloaded when they change, for the listener and for connections to peers alike, so a rotated Secret takes effect without restarting the pod. If a new set is unusable (a half-finished rotation, say) SIC logs it and keeps the previous one. The Kubernetes service account token used for peer discovery is likewise read afresh for every request.

A cache serves any client that presents a valid certificate. It does not check registry credentials, so a blob that was pulled with credentials can be read by anyone holding a client certificate and the blob's digest. Treat the client CA as the access boundary.

## Build

```bash
go mod download
go test ./...
docker build -t steady-image-cache:v3 .
```

The build requires the containerd Go client dependency declared in `go.mod`.

## Endpoints

* `GET /v2/`
* `GET|HEAD /v2/<repo>/blobs/<digest>` (including byte ranges)
* `GET|HEAD /v2/<repo>/manifests/<reference>`
* `GET /metrics`
* `GET /healthz`

## Deployment

`deploy/daemonset.yaml` is a starting point, not a finished product; it needs the TLS secret `steady-image-cache-tls` and an image reference you have chosen. What it does, and why:

* **Image.** `ghcr.io/steadyinfratech/steady-image-cache:main` is the branch build. Pin a release tag or an `@sha256:` digest. `imagePullPolicy` stays `IfNotPresent` (see Bootstrap).
* **Root, with nothing else.** The container runs as root only because containerd's socket is normally `root:root 0660`. It drops every capability, cannot gain privileges, has a read-only root file system and the runtime's default seccomp profile. If you turn the containerd import off (`SIC_CONTAINERD_IMPORT=false`) it needs no socket and can run as a non-root user that owns the cache directory.
* **Resources.** Requests of 50m CPU and 64Mi memory; a memory limit of 512Mi to catch a leak. There is no CPU limit on purpose: hashing a layer is bursty, and throttling only makes pulls slower.
* **Probes.** Readiness and liveness use `/healthz` on the plain-HTTP admin port, because kubelet cannot present the client certificate the main port wants. The liveness probe waits for a minute of failures before restarting: a restart interrupts transfers under way.
* **Scheduling.** `system-node-critical`, so node pressure does not evict the cache, and a toleration for every taint, so it also runs on control-plane, GPU and spot nodes. Narrow the toleration to keep it off some nodes. If you lower the priority class, a node that is short of memory may lose its cache pod; pulls still work, but go to the registry.
* **Cache size.** Unset, the cache may use the disk until 20% and 1 GiB remain, then evicts (see "Cache size and eviction"). A commented `SIC_MAX_CACHE_BYTES` shows how to cap it.

CI validates the manifest against the Kubernetes schemas (strictly, for 1.28 and 1.31) and runs `deploy/check_manifest.py`, which checks that ports, probes and labels agree with each other and with SIC's settings and that the security settings stay in place.

### Bootstrap

With `deploy/containerd/hosts.toml` as the `_default` configuration, SIC is the only host containerd tries. If SIC is not running on a node, nothing can be pulled there, SIC's own image included. So after a node restart or a node-image upgrade the image must already be on the node: keep `imagePullPolicy: IfNotPresent`, pin the tag or digest so a changed tag does not need a pull, and bake the SIC image into the node image or pre-pull it before `hosts.toml` is installed. Restricting `hosts.toml` to the registries you cache, instead of `_default`, leaves the others (the one that holds SIC's image, say) on the normal path.

## Production notes

SIC verifies every imported or downloaded blob against its SHA-256 digest before accepting it. The containerd store is only accessed through its gRPC API; SIC does not modify containerd content. Its own cache is independent under `/var/lib/steady-image-cache`.

Known limits: the containerd import copies every blob into SIC's own cache, so each image occupies disk space twice on a node.

For a large cluster, the next scalability step would be peer indexing (Bloom filters or a digest advertisement protocol) so a miss does not probe peers sequentially. The current implementation intentionally keeps discovery simple and deterministic.
