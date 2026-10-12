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
* Upstream fallback remains optional and is disabled by default.

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

## Registry mirror

See `deploy/containerd/hosts.toml`. Install it as the `_default/hosts.toml` registry configuration on each node, adjusting certificate paths for your environment.

## TLS

The sample DaemonSet expects a `steady-image-cache-tls` Secret containing:

* `tls.crt`
* `tls.key`
* `ca.crt`

Peer certificates should contain the DNS SAN configured by `SIC_TLS_SERVER_NAME` (default `steady-image-cache`). Peers are discovered by IP, but TLS verifies this stable server name.

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

For a large cluster, the next scalability step would be peer indexing (Bloom filters or a digest advertisement protocol) so a miss does not probe peers sequentially. The current implementation intentionally keeps discovery simple and deterministic.
