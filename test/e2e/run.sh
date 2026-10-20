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
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=${CLUSTER:-sic-e2e}
KEEP=${KEEP:-}
SIC_IMAGE=ghcr.io/steadyinfratech/steady-image-cache:main  # what daemonset.yaml names
APP=registry.example.test/e2e/app                          # never resolvable
WORK=$(mktemp -d)
export KUBECONFIG=$WORK/kubeconfig

log() { printf '\n=== %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

dump() {
  log "diagnostics"
  kubectl -n kube-system get pods -o wide || true
  for p in $(kubectl -n kube-system get pods -l app=steady-image-cache -o name 2>/dev/null); do
    echo "--- $p"; kubectl -n kube-system logs "$p" --tail=80 || true
  done
  for n in "${NODES[@]:-}"; do
    [ -n "$n" ] || continue
    echo "--- containerd on $n"; docker exec "$n" journalctl -u containerd --no-pager -n 40 || true
  done
}

cleanup() {
  rc=$?
  [ $rc -eq 0 ] || dump
  if [ -z "$KEEP" ]; then kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
  rm -rf "$WORK"
  exit $rc
}
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

log "build the SIC image"
docker build -t "$SIC_IMAGE" "$ROOT"

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

log "wait for SIC on $A to import it"
imported() { [ "$(metric "$A" sic_containerd_imported_blobs_total)" -ge 4 ]; }
wait_for "the import on $A" 90 imported
echo "imported blobs on $A: $(metric "$A" sic_containerd_imported_blobs_total)"

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

log "pass: $B pulled the image from $A through SIC"
