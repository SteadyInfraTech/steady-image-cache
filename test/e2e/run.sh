#!/usr/bin/env bash
# End-to-end test on kind: a real containerd, SIC deployed from
# deploy/daemonset.yaml, and the premise of the project.
#
#   Node A already has an image in containerd (as if a pod had pulled it).
#   SIC on A imports it. Node B pulls the same image by digest and has to get
#   every blob from A's SIC. No registry is reachable or configured, so a pull
#   that works can only have come from the peer.
#
# What this covers that the unit tests cannot: the Kubernetes peer discovery,
# the containerd import against a real containerd, hosts.toml with mutual TLS,
# and the sample DaemonSet itself.
#
# Needs docker, kind, kubectl, openssl. Run from anywhere:
#   test/e2e/run.sh            # creates the cluster "sic-e2e" and deletes it
#   KEEP=1 test/e2e/run.sh     # leave the cluster for inspection
#   CLUSTER=foo test/e2e/run.sh
#   MODE=registries test/e2e/run.sh
#                              instead of the self-contained test, pull from real
#                              public registries (needs the internet); see
#                              .github/workflows/e2e-registries.yml
set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=${CLUSTER:-sic-e2e}
KEEP=${KEEP:-}
MODE=${MODE:-local}
case $MODE in local|registries) ;; *) echo "MODE must be local or registries" >&2; exit 1 ;; esac
# The image the manifest names, so that bumping the tag there for a release does
# not need a change here. It is built locally and loaded into the nodes.
SIC_IMAGE=$(awk '/^ *image: ghcr.io\/steadyinfratech\/steady-image-cache[:@]/ {print $2; exit}' "$ROOT/deploy/daemonset.yaml")
[ -n "$SIC_IMAGE" ] || { echo "no SIC image in deploy/daemonset.yaml" >&2; exit 1; }
APP=registry.example.test/e2e/app                          # never resolvable
REGISTRY=sic-e2e-registry
REGISTRY_HOST=registry.e2e.test                            # resolves only on the nodes, see phase 2
# The registry that plays the upstream. The ECR mirror has no pull rate limit.
REGISTRY_IMAGES=(public.ecr.aws/docker/library/registry:2 registry:2)
WORK=$(mktemp -d)
export KUBECONFIG=$WORK/kubeconfig

STAGE=start
log() { STAGE=$*; printf '\n=== %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

# Everything printed is also kept, so that a failure can be reported as a
# GitHub annotation: those are readable from the API and the checks page even
# when the job log is not at hand.
OUT=$WORK/output.log
exec > >(tee "$OUT") 2>&1

# annotate TITLE: stdin, trimmed to its tail, as a workflow error annotation.
annotate() {
  [ -n "${GITHUB_ACTIONS:-}" ] || return 0
  local body
  body=$(tail -c 3000 | sed -e 's/%/%25/g' -e 's/\r//g' | awk 'BEGIN {ORS="%0A"} {print}')
  echo "::error title=$1::$body"
}

dump() {
  log "diagnostics"
  { echo "stage: $STAGE"; echo "command: $BASH_COMMAND_FAILED (line $LINE_FAILED)"; tail -n 25 "$OUT"; } | annotate "e2e failed"
  kubectl -n kube-system get pods -o wide 2>&1 | tee "$WORK/pods.txt" || true
  annotate "pods" < "$WORK/pods.txt"
  for p in $(kubectl -n kube-system get pods -l app=steady-image-cache -o name 2>/dev/null); do
    echo "--- $p"
    { kubectl -n kube-system get "$p" -o wide --no-headers; kubectl -n kube-system logs "$p" --tail=40; } 2>&1 | tee "$WORK/pod.txt" || true
    annotate "$p" < "$WORK/pod.txt"
  done
  if docker inspect "$REGISTRY" >/dev/null 2>&1; then
    echo "--- registry"
    docker logs --tail 25 "$REGISTRY" 2>&1 | tee "$WORK/reg.txt" || true
    annotate "registry" < "$WORK/reg.txt"
  fi
  for n in "${NODES[@]:-}"; do
    [ -n "$n" ] || continue
    echo "--- containerd on $n"
    docker exec "$n" journalctl -u containerd --no-pager -n 25 2>&1 | tee "$WORK/cd.txt" || true
    annotate "containerd $n" < "$WORK/cd.txt"
  done
}

cleanup() {
  rc=$?
  trap - ERR
  if [ $rc -ne 0 ]; then dump; sleep 1; fi
  if [ -z "$KEEP" ]; then kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
  docker rm -f "$REGISTRY" >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit $rc
}
BASH_COMMAND_FAILED=; LINE_FAILED=
trap 'BASH_COMMAND_FAILED=$BASH_COMMAND; LINE_FAILED=$LINENO' ERR
NODES=()
trap cleanup EXIT

# metric NODE NAME: the value of a counter on that node's admin port.
metric() {
  docker exec "$1" curl -fsS http://127.0.0.1:5001/metrics | awk -v n="$2" '$1==n {print $2; found=1} END {if (!found) print 0}'
}

# wait_for DESCRIPTION SECONDS COMMAND...
wait_for() {
  local what=$1 secs=$2; shift 2
  for _ in $(seq "$secs"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  fail "timed out after ${secs}s waiting for $what"
}

# registries_phase: what only a real registry can show. Public registries
# answer an anonymous request with 401 and a Bearer challenge; SIC passes it to
# containerd, which gets a token from the registry's own token service. No
# credentials are involved. The image's built-in CA roots are what verify
# these registries, since no private CA is configured here.
registries_phase() {
  log "registries: upstream on for public registries, no private CA"
  kubectl -n kube-system set env daemonset/steady-image-cache \
    SIC_UPSTREAM_ENABLED=true SIC_UPSTREAM_REGISTRIES="public.ecr.aws,ghcr.io"
  kubectl -n kube-system rollout status daemonset/steady-image-cache --timeout=180s
  both_see_peers() { [ "$(metric "$A" sic_peers)" -ge 1 ] && [ "$(metric "$B" sic_peers)" -ge 1 ]; }
  wait_for "both workers to see a peer" 90 both_see_peers

  local img
  for img in public.ecr.aws/docker/library/busybox:latest ghcr.io/jqlang/jq:latest; do
    log "registries: $img on $A, then on $B"
    local ch_before up_before peer_before
    ch_before=$(metric "$A" sic_upstream_auth_challenges_total)
    up_before=$(metric "$A" sic_upstream_hits_total)
    docker exec "$A" crictl pull "$img"
    echo "challenges relayed on $A: $ch_before -> $(metric "$A" sic_upstream_auth_challenges_total)"
    echo "upstream fetches on $A:   $up_before -> $(metric "$A" sic_upstream_hits_total)"
    [ "$(metric "$A" sic_upstream_auth_challenges_total)" -gt "$ch_before" ] || fail "$img: no authentication challenge was relayed on $A"
    [ $(( $(metric "$A" sic_upstream_hits_total) - up_before )) -ge 2 ] || fail "$img: $A fetched fewer than 2 objects from the registry"

    peer_before=$(metric "$B" sic_peer_hits_total)
    docker exec "$B" crictl pull "$img"
    echo "peer hits on $B: $peer_before -> $(metric "$B" sic_peer_hits_total)"
    [ $(( $(metric "$B" sic_peer_hits_total) - peer_before )) -ge 2 ] || fail "$img: $B got fewer than 2 blobs from its peer"
  done
  [ "$(metric "$B" sic_verify_errors_total)" = 0 ] || fail "$B had verification errors"
  [ "$(metric "$B" sic_peer_errors_total)" = 0 ] || fail "$B had peer errors"
  log "pass: public registries over their own token services, then shared between nodes"
}

log "build the SIC image"
docker build --build-arg VERSION=e2e -t "$SIC_IMAGE" "$ROOT"

log "create the kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --config "$ROOT/test/e2e/kind-config.yaml" --kubeconfig "$KUBECONFIG" --wait 120s
mapfile -t NODES < <(kind get nodes --name "$CLUSTER")
A=$CLUSTER-worker
B=$CLUSTER-worker2
for n in "$A" "$B"; do printf '%s\n' "${NODES[@]}" | grep -qx "$n" || fail "expected node $n, have: ${NODES[*]}"; done
kind load docker-image "$SIC_IMAGE" --name "$CLUSTER"

log "certificates (the recipe from the README, plus a client-only one for containerd)"
(
  cd "$WORK"
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 -subj "/CN=sic-ca" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign" -keyout ca.key -out ca.crt 2>/dev/null
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=steady-image-cache" -keyout tls.key -out tls.csr 2>/dev/null
  openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -out tls.crt -extfile <(printf '%s\n' \
    'subjectAltName=DNS:steady-image-cache,IP:127.0.0.1' 'extendedKeyUsage=serverAuth,clientAuth' 'keyUsage=critical,digitalSignature') 2>/dev/null
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=containerd" -keyout client.key -out client.csr 2>/dev/null
  openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -out client.crt -extfile <(printf '%s\n' \
    'extendedKeyUsage=clientAuth' 'keyUsage=critical,digitalSignature') 2>/dev/null
)

kubectl -n kube-system create secret generic steady-image-cache-tls \
  --from-file=tls.crt="$WORK/tls.crt" --from-file=tls.key="$WORK/tls.key" --from-file=ca.crt="$WORK/ca.crt"

log "containerd's side: hosts.toml and the files it names, on every node"
# deploy/containerd/hosts.toml names /etc/steady-image-cache/tls.{crt,key}; here
# containerd gets its own client-only pair under those names.
for n in "${NODES[@]}"; do
  docker exec "$n" mkdir -p /etc/containerd/certs.d/_default /etc/steady-image-cache
  docker cp "$ROOT/deploy/containerd/hosts.toml" "$n:/etc/containerd/certs.d/_default/hosts.toml"
  docker cp "$WORK/ca.crt" "$n:/etc/steady-image-cache/ca.crt"
  docker cp "$WORK/client.crt" "$n:/etc/steady-image-cache/tls.crt"
  docker cp "$WORK/client.key" "$n:/etc/steady-image-cache/tls.key"
done

log "deploy SIC from deploy/daemonset.yaml"
kubectl apply -f "$ROOT/deploy/daemonset.yaml"
# Only the interval differs from the sample: the default is five minutes.
kubectl -n kube-system set env daemonset/steady-image-cache SIC_IMPORT_INTERVAL=3s
kubectl -n kube-system rollout status daemonset/steady-image-cache --timeout=180s

if [ "$MODE" = registries ]; then registries_phase; exit 0; fi

log "an image that only node $A has"
mkdir "$WORK/app"
# Random layers: nothing about them can already be in any cache.
head -c 3000000 /dev/urandom > "$WORK/app/one"
head -c 2000000 /dev/urandom > "$WORK/app/two"
printf 'FROM scratch\nCOPY one /one\nCOPY two /two\n' > "$WORK/app/Dockerfile"
docker build -t "$APP:v1" "$WORK/app"
kind load docker-image "$APP:v1" --name "$CLUSTER" --nodes "$A"
DIGEST=$(docker exec "$A" ctr -n k8s.io images ls "name==$APP:v1" | awk 'NR==2 {print $3}')
[[ $DIGEST == sha256:* ]] || fail "could not read the image digest on $A (got '$DIGEST')"
echo "image $APP@$DIGEST"
if docker exec "$B" ctr -n k8s.io images ls -q | grep -q "$APP"; then fail "node $B should not have the image yet"; fi

log "wait for SIC on $A to hold the image, and for $B to see a peer"
# Not the import counter: kind's own images are imported too. Ask A's SIC for
# this manifest over mutual TLS, the way containerd would.
serves() {
  docker exec "$A" curl -fsS -o /dev/null -I --cacert /etc/steady-image-cache/ca.crt \
    --cert /etc/steady-image-cache/tls.crt --key /etc/steady-image-cache/tls.key \
    "https://127.0.0.1:5000/v2/e2e/app/manifests/$DIGEST?ns=registry.example.test"
}
wait_for "$A's SIC to serve the manifest" 90 serves
# Peers are listed every 30s, and a pod that was not Ready at the last listing
# is missing from it.
sees_peer() { [ "$(metric "$B" sic_peers)" -ge 1 ]; }
wait_for "$B's SIC to see a peer" 90 sees_peer
echo "imported blobs on $A: $(metric "$A" sic_containerd_imported_blobs_total); peers seen by $B: $(metric "$B" sic_peers)"

log "pull on $B by digest, through SIC"
before_peer=$(metric "$B" sic_peer_hits_total)
before_local=$(metric "$A" sic_local_hits_total)
docker exec "$B" crictl pull "$APP@$DIGEST"

after_peer=$(metric "$B" sic_peer_hits_total)
after_local=$(metric "$A" sic_local_hits_total)
echo "peer hits on $B:  $before_peer -> $after_peer"
echo "local hits on $A: $before_local -> $after_local"
echo "upstream hits on $B: $(metric "$B" sic_upstream_hits_total)"

docker exec "$B" ctr -n k8s.io images ls -q | grep -q "$APP@$DIGEST" || fail "$B does not have the image after the pull"
# A manifest, a config and two layers.
[ $((after_peer - before_peer)) -ge 4 ] || fail "expected at least 4 peer hits on $B (manifest, config, 2 layers), got $((after_peer - before_peer))"
[ "$after_local" -gt "$before_local" ] || fail "$A's SIC did not serve anything"
[ "$(metric "$B" sic_upstream_hits_total)" = 0 ] || fail "$B contacted an upstream registry"
[ "$(metric "$B" sic_peer_errors_total)" = 0 ] || fail "$B had peer errors"
[ "$(metric "$B" sic_verify_errors_total)" = 0 ] || fail "$B had verification errors"

log "phase 1 passed: $B pulled the image from $A through SIC"

# ---------------------------------------------------------------------------
# Phase 2: a real registry behind a private CA as the upstream.
#
# Covers what phase 1 cannot: upstream fallback, the private CA reaching SIC
# through SSL_CERT_FILE, a tag that moves, and a registry that goes away.
# ---------------------------------------------------------------------------
UP=$REGISTRY_HOST/e2e/up
LOCAL=localhost:5443/e2e/up  # how the host reaches the same registry

log "phase 2: an HTTPS registry with a certificate from the same CA"
(
  cd "$WORK"
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$REGISTRY_HOST" -keyout reg.key -out reg.csr 2>/dev/null
  openssl x509 -req -in reg.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -out reg.crt -extfile <(printf '%s\n' \
    "subjectAltName=DNS:$REGISTRY_HOST" 'extendedKeyUsage=serverAuth' 'keyUsage=critical,digitalSignature') 2>/dev/null
)
for img in "${REGISTRY_IMAGES[@]}"; do
  if docker pull -q "$img" >/dev/null 2>&1; then REGISTRY_IMAGE=$img; break; fi
done
[ -n "${REGISTRY_IMAGE:-}" ] || fail "could not pull a registry image (tried: ${REGISTRY_IMAGES[*]})"
docker rm -f "$REGISTRY" >/dev/null 2>&1 || true
docker run -d --name "$REGISTRY" --network kind -p 127.0.0.1:5443:443 -v "$WORK:/certs:ro" \
  -e REGISTRY_HTTP_ADDR=0.0.0.0:443 -e REGISTRY_HTTP_TLS_CERTIFICATE=/certs/reg.crt -e REGISTRY_HTTP_TLS_KEY=/certs/reg.key \
  "$REGISTRY_IMAGE" >/dev/null
registry_up() { curl -fsS -o /dev/null --cacert "$WORK/ca.crt" --resolve "$REGISTRY_HOST:5443:127.0.0.1" "https://$REGISTRY_HOST:5443/v2/"; }
wait_for "the registry" 60 registry_up
REGISTRY_IP=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$REGISTRY")
[ -n "$REGISTRY_IP" ] || fail "no address for the registry container"
# Only the nodes know the name; SIC runs on the host network, so it resolves it
# from the node's /etc/hosts.
for n in "${NODES[@]}"; do docker exec "$n" sh -c "echo '$REGISTRY_IP $REGISTRY_HOST' >> /etc/hosts"; done

# push NAME: a new image with random layers under $LOCAL:v1; prints its digest.
# The docker daemon treats localhost registries as insecure, so it does not
# need the test CA.
push() {
  local dir=$WORK/up-$1
  mkdir -p "$dir"
  head -c 3000000 /dev/urandom > "$dir/one"
  head -c 2000000 /dev/urandom > "$dir/two"
  printf 'FROM scratch\nCOPY one /one\nCOPY two /two\n' > "$dir/Dockerfile"
  docker build -q -t "$LOCAL:v1" "$dir" >/dev/null
  docker push "$LOCAL:v1" | awk '/digest: sha256:/ {print $3}'
}
registry_blob_gets() { docker logs "$REGISTRY" 2>&1 | grep -c 'GET /v2/e2e/up/blobs/sha256:' || true; }
node_digest() { docker exec "$1" ctr -n k8s.io images ls "name==$UP:v1" | awk 'NR==2 {print $3}'; }

log "point SIC at the registry: upstream on, private CA through SSL_CERT_FILE, short tag TTL"
kubectl -n kube-system create configmap upstream-ca --from-file=ca-bundle.crt="$WORK/ca.crt"
kubectl -n kube-system patch daemonset steady-image-cache --type=json -p '[
  {"op":"add","path":"/spec/template/spec/volumes/-","value":{"name":"upstream-ca","configMap":{"name":"upstream-ca"}}},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"upstream-ca","mountPath":"/etc/sic-roots","readOnly":true}}]'
kubectl -n kube-system set env daemonset/steady-image-cache \
  SIC_UPSTREAM_ENABLED=true SIC_UPSTREAM_REGISTRIES="$REGISTRY_HOST" SSL_CERT_FILE=/etc/sic-roots/ca-bundle.crt SIC_TAG_TTL=3s
kubectl -n kube-system rollout status daemonset/steady-image-cache --timeout=180s
both_see_peers() { [ "$(metric "$A" sic_peers)" -ge 1 ] && [ "$(metric "$B" sic_peers)" -ge 1 ]; }
wait_for "both workers to see a peer" 90 both_see_peers

D1=$(push first)
[[ $D1 == sha256:* ]] || fail "could not push the first image (digest '$D1')"
echo "pushed $UP:v1 = $D1"

log "pull the tag on $A: from the registry, through SIC, over the private CA"
docker exec "$A" crictl pull "$UP:v1"
[ "$(node_digest "$A")" = "$D1" ] || fail "$A has $(node_digest "$A") for the tag, want $D1"
[ "$(metric "$A" sic_upstream_hits_total)" -ge 3 ] || fail "$A made $(metric "$A" sic_upstream_hits_total) upstream fetches, want at least 3 (config and two layers)"
gets_after_a=$(registry_blob_gets)
[ "$gets_after_a" -ge 3 ] || fail "the registry's log shows $gets_after_a blob requests after $A's pull, want at least 3 (is the log format as expected?)"

log "pull the same tag on $B: blobs from $A, none from the registry"
peer_before=$(metric "$B" sic_peer_hits_total)
docker exec "$B" crictl pull "$UP:v1"
[ "$(node_digest "$B")" = "$D1" ] || fail "$B has $(node_digest "$B") for the tag, want $D1"
[ "$(registry_blob_gets)" = "$gets_after_a" ] || fail "the registry served blobs again for $B ($gets_after_a -> $(registry_blob_gets))"
peer_gain=$(( $(metric "$B" sic_peer_hits_total) - peer_before ))
[ "$peer_gain" -ge 3 ] || fail "$B had $peer_gain peer hits, want at least 3"

log "the tag moves: a node must not keep serving the old digest"
D2=$(push second)
[ "$D2" != "$D1" ] || fail "the second image has the first one's digest"
sleep 4  # longer than SIC_TAG_TTL
docker exec "$A" crictl pull "$UP:v1"
[ "$(node_digest "$A")" = "$D2" ] || fail "$A still has $(node_digest "$A") for the moved tag, want $D2"
[ "$(metric "$A" sic_tag_changes_total)" -ge 1 ] || fail "$A did not count a tag change"

log "the registry goes away: a cached tag is still served, and counted as stale"
docker stop "$REGISTRY" >/dev/null
sleep 4
stale_before=$(metric "$B" sic_stale_tags_served_total)
docker exec "$B" crictl pull "$UP:v1"
[ "$(node_digest "$B")" = "$D1" ] || fail "$B has $(node_digest "$B") after the registry went away, want its cached $D1"
[ "$(metric "$B" sic_stale_tags_served_total)" -gt "$stale_before" ] || fail "$B did not count a stale tag served"

log "pass: peers, upstream over a private CA, a moving tag, and a registry that is gone"
