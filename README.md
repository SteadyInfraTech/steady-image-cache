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

The containerd account/socket permissions must allow the SIC process to call the content and image services. The supplied DaemonSet runs the container as UID 0 for that reason; tighten this if your containerd socket group/ACL permits it.

## Peer requests

Requests that SIC sends to its peers carry an `X-SIC-Peer` header. A SIC that receives one answers from its own cache only and never forwards it to other peers or upstream, so two nodes that both miss cannot ask each other in a loop.

A peer that does not accept a connection and start answering within `SIC_PEER_TIMEOUT` (default `5s`) is skipped for `SIC_PEER_BACKOFF` (default `30s`; `0` disables skipping), so a dead node slows one request instead of every miss. Peers discovered through Kubernetes must be Ready and not terminating.

## Download limits

A digest can only be checked once a download has finished, so SIC bounds what it will accept before then:

* A blob from a peer or registry must declare its `Content-Length`; that length is the most SIC will write, and it must fit on the disk.
* A manifest may be at most 4 MiB. Its digest is computed from its content; a `Docker-Content-Digest` header is only cross-checked, never required.
* `SIC_MIN_FREE_BYTES` (default 1 GiB, `0` for none) is the free disk space that has to remain after a download. When it would not, the download is refused. This also applies to the containerd import.
* Responses are never decompressed on the way in, so the stored bytes are exactly what the digest covers.

## Upstream fallback

Set `SIC_UPSTREAM_ENABLED=true` and list the registries SIC may contact in `SIC_UPSTREAM_REGISTRIES` (comma separated, e.g. `docker.io,ghcr.io`). The registry is taken from the `?ns=` parameter of the request; a registry that is not listed is refused. Upstream connections are verified against the system CA roots (the peer CA is only used between SIC nodes), so the image needs a CA bundle in that case. The `scratch` base image has none.

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

## Production notes

SIC verifies every imported or downloaded blob against its SHA-256 digest before accepting it. The containerd store is only accessed through its gRPC API; SIC does not modify containerd content. Its own cache is independent under `/var/lib/steady-image-cache`.

Known limits: the containerd import copies every blob into SIC's own cache, so each image occupies disk space twice on a node, and a blob that cannot be imported is skipped without a log line. There is no cache eviction.

For a large cluster, the next scalability step would be peer indexing (Bloom filters or a digest advertisement protocol) so a miss does not probe peers sequentially. The current implementation intentionally keeps discovery simple and deterministic.
