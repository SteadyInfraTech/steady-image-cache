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

**Rollouts.** A peer is also served a blob that this node is still downloading, as long as the download has already delivered bytes: when a new image goes out, the first node's pull is passed down the line while it runs, instead of every other node waiting for it to finish and then going to the registry. A peer is never made to wait for a download to start, so two nodes cannot end up waiting for each other. A request for a blob that is neither cached nor started gets a `404`, as before. `sic_peer_streams_total` counts peer requests answered this way. The last-byte rule from "Streaming a miss" applies to peers as well, and a peer that stops halfway is replaced by the next one for later requests.

**How many peers a miss asks.** Peers are asked one after another, in random order, and a miss asks at most `SIC_PEER_MAX_TRIES` of them (default `4`; `0` asks all). Without a limit, a blob that nobody has costs one round trip per node before it reaches the registry. The price is that on a cluster larger than the limit, a blob that only a few nodes hold can be missed: with `N` peers of which `k` hold the blob, a miss that asks `m` of them finds one with probability `1 - C(N-k, m) / C(N, m)`; for a single holder that is `m/N`, so one holder among 50 peers is found by `4` in `50` misses. During a rollout the number of holders grows with every pull, which is what the limit relies on. Raise it if your registry is slow or expensive and your nodes are close together. `sic_peer_cap_reached_total` counts misses that were cut short, and `sic_peers` is the number of peers a node currently sees.

**Redirects.** A peer never legitimately redirects: it has the blob or it does not. SIC does not follow a peer's redirect, which would let a node with a valid certificate send this one to any address, plain HTTP included. A peer that answers with one is treated as faulty, logged, and skipped for `SIC_PEER_BACKOFF`. A registry's redirects, on the other hand, are normal (blobs often come from a CDN or an object store), so SIC follows up to 5 of them, over `https` only; a redirect to plain HTTP or a longer chain fails the fetch and is logged. As usual with `net/http`, the `Authorization` header is not sent to a different host than the one it was meant for.

## Streaming a miss

A layer that is not cached is passed on to containerd while it is still being downloaded, so the pull starts at once instead of waiting for the whole layer to arrive first. Everyone asking for the same blob shares one download, which goes on if a client leaves, and every one of them is fed from the file as it is written.

The digest can only be checked at the end, so the last byte of a response is held back until the check has passed. If it fails, or the source breaks off, the response stops one byte short and the connection is closed: containerd sees an error and retries, and the bad content is never stored. The earlier bytes were sent unverified; containerd checks every layer against its digest itself. A request with a `Range` header needs the whole blob and waits for it. A `HEAD` for a blob that is not cached and not being downloaded is passed to the peers and then the upstream registry as a `HEAD`, and their size is relayed; it never starts a download. A `HEAD` while a download is running waits for it. `sic_streamed_responses_total` counts streamed responses and `sic_stream_aborts_total` those that were cut short.

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

### Registries that need a token

Most registries, Docker Hub and GHCR among them, answer an anonymous request with `401` and a `WWW-Authenticate` challenge that says where to get a token. SIC passes that challenge on, unchanged, to containerd (up to four values of at most 4 KiB each). containerd fetches a token from the registry's own authentication service and repeats the request with `Authorization: Bearer ...`, which SIC forwards to the registry. The content is then cached like any other. Credentials never rest in SIC: the `Authorization` header is only passed on, and is not stored or logged. A rejected or expired token gets the challenge again. `sic_upstream_auth_challenges_total` counts the challenges relayed; they are not logged, since every anonymous pull starts with one.

A `401` without a `WWW-Authenticate` header cannot be acted on, so it ends as a `404` (which sends containerd on to its next host) and is logged.

**The cache does not check who is asking.** Once a blob or tag has been fetched with someone's credentials, any client that can reach this node's SIC, and knows the digest or tag, gets it without credentials. For private images that means every client the listener accepts can read them: with mTLS, every holder of a certificate signed by your CA; with `SIC_INSECURE=true`, anyone who can reach the port. Keep upstream fallback off for registries whose content must not be shared on the node.

### Tags: staying current, and who is believed

A digest never changes, but a tag such as `latest` does, and a tag has no digest to check an answer against. SIC handles the two ways a cached tag can be wrong.

**Out of date.** When the tag's registry can be asked (upstream fallback on, registry listed), a cached tag older than `SIC_TAG_TTL` (default `10m`; `0` never asks again) is confirmed first: SIC sends the registry a `HEAD` for the manifest, which returns the digest and, on Docker Hub, does not count against pull rate limits. If the tag still points at the same digest it is served and the clock restarts; if it moved, the new manifest is fetched. `sic_tag_checks_total` counts the checks and `sic_tag_changes_total` the tags found to have moved. An alias that came from containerd was never confirmed, so it is checked at its first use, and the periodic import never replaces an alias the cache already has.

**False.** One faulty or compromised node could otherwise tell every other node that `latest` is some other image. So when the registry can be asked, *it* says what a tag points at; the manifest is then fetched by digest, from a peer if one has it, and checked against that digest. Peers decide what a tag means only when the registry cannot be asked: air-gapped nodes, or a registry that is not listed. There the CA that vouches for the peers is the trust boundary.

When the registry cannot confirm a tag (unreachable, or it wants credentials the client did not send), SIC serves the cached one rather than fail the pull, logs it at most once a minute, and counts it in `sic_stale_tags_served_total`. A tag the registry no longer has stays cached. Requests from other nodes are answered from the cache whatever the tag's age, since they cannot be sent anywhere better. The time of each confirmation is kept with the record of verified blobs; losing it only means tags are checked again.

## Failures

When SIC cannot cache something it says why, instead of answering 404 as if the image did not exist:

* `507` when the free disk space would drop below `SIC_MIN_FREE_BYTES` (or the disk is full); counted in `sic_disk_full_total`.
* `500` when the cache directory cannot be written; counted in `sic_store_errors_total`.
* `400` for a repository name that is not valid per the OCI distribution spec. This is the caller's mistake and does not take any peer out of rotation.

Both conditions, and non-404 answers from peers and upstream registries, are logged at most once per minute per cause, with a count of the lines held back.

## Health and metrics

With mTLS enabled the main listener requires a client certificate on every path, including `/healthz`, which kubelet cannot present. Set `SIC_ADMIN_LISTEN` (the sample DaemonSet uses `127.0.0.1:5001`) to serve `/healthz` and `/metrics` over plain HTTP on that address as well.

`sic_peers` is the number of peers this node would currently ask. Kubernetes discovery lists the pods every 30 seconds and keeps only Ready ones, so a node that starts together with the others sees no peers for up to 30 seconds, and a newly Ready neighbour is not asked until the next listing. Until then a miss goes straight to the registry (or fails, if upstream fallback is off).

## Registry mirror

See `deploy/containerd/hosts.toml`. Install it as the `_default/hosts.toml` registry configuration on each node, adjusting certificate paths for your environment.

## TLS

The sample DaemonSet expects a `steady-image-cache-tls` Secret containing:

* `tls.crt`
* `tls.key`
* `ca.crt`

### What the certificate must contain

A node uses one certificate for three things, and each has a requirement that fails with a handshake error that does not say why:

| Used for | Requirement |
| --- | --- |
| Peers calling this node | A DNS SAN equal to `SIC_TLS_SERVER_NAME` (default `steady-image-cache`). Peers are found by IP address but verify this name, so **no IP SAN is needed for peers**. The extended key usage must allow **server auth**. |
| This node calling peers | The same certificate is presented as a client certificate, so the extended key usage must also allow **client auth**. cert-manager's default usages do not include it. A certificate with no extended key usage at all is allowed for everything. |
| containerd calling this node | `deploy/containerd/hosts.toml` names SIC as `https://127.0.0.1:5000`, and containerd checks that address: the certificate needs the **IP SAN `127.0.0.1`**. (hosts.toml has no setting to check a name instead.) |

All of them must be signed by the CA in `ca.crt` (an intermediate CA is fine if `tls.crt` carries it after the certificate) and be in date.

SIC checks its own certificate when it starts and after every rotation, and logs what it finds as `tls: certificate problem: ...`, with what to change. It still uses the certificate: a warning does not stop a pull that works. The IP SAN message is only a warning because only containerd needs it. A certificate that expires within 14 days is reported too, which shows that renewal has stopped.

A certificate that satisfies all of this, made with OpenSSL (a throwaway CA; use your own PKI in production):

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 -subj "/CN=sic-ca" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=steady-image-cache" -keyout tls.key -out tls.csr
openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -out tls.crt -extfile <(printf '%s\n' \
  'subjectAltName=DNS:steady-image-cache,IP:127.0.0.1' 'extendedKeyUsage=serverAuth,clientAuth' 'keyUsage=critical,digitalSignature')
kubectl -n kube-system create secret generic steady-image-cache-tls --from-file=tls.crt --from-file=tls.key --from-file=ca.crt
```

With cert-manager, the part that matters in a `Certificate` is:

```yaml
spec:
  secretName: steady-image-cache-tls
  dnsNames: [steady-image-cache]
  ipAddresses: [127.0.0.1]
  usages: [digital signature, key encipherment, server auth, client auth]
```

containerd presents a client certificate too (`client` in `hosts.toml`), from files on the node (`/etc/steady-image-cache/` in the sample). Getting them there is up to your node provisioning; the Secret is only mounted into the pod. Give containerd a certificate of its own from the same CA, with only the **client auth** usage, rather than a copy of the node's server key: the files are readable by anything on the node that can read `/etc`.

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

### End-to-end test

`test/e2e/run.sh` runs SIC on a three-node [kind](https://kind.sigs.k8s.io/) cluster, deployed from `deploy/daemonset.yaml` with a real containerd. One worker already has an image; SIC imports it. The other worker pulls the image by digest through SIC, with no registry reachable at all, so the pull can only succeed if peer discovery, mutual TLS and the import all work. It then checks the peer-hit counters. It needs `docker`, `kind`, `kubectl` and `openssl`, takes a few minutes, and runs in CI (`.github/workflows/e2e.yml`) when code or deployment files change. `KEEP=1 test/e2e/run.sh` leaves the cluster up for inspection.

It does not cover the upstream fallback or tag freshness; those are tested in the unit tests against a fake registry.

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
