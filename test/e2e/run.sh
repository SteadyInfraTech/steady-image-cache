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
# Later phases (see below) put registries behind SIC, and the last one installs
# the sample mirror configuration, deploy/containerd/mirror-hosts.toml, for a
# registry SIC may not contact: containerd must fall back to the registry when
# SIC does not have the content or is not reachable, and must not take a moved
# tag from an old cached alias.
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
HTTPD_IMAGES=(public.ecr.aws/docker/library/httpd:2 httpd:2)  # only for its htpasswd
PRIVATE=sic-e2e-registry-private
PRIVATE_HOST=private.e2e.test                              # a registry that wants credentials
MIRROR=sic-e2e-registry-mirror
MIRROR_HOST=mirror.e2e.test                                # a registry SIC may NOT contact (phase 4)
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
  if docker inspect "$PRIVATE" >/dev/null 2>&1; then
    echo "--- private registry"
    docker logs --tail 25 "$PRIVATE" 2>&1 | tee "$WORK/preg.txt" || true
    annotate "private registry" < "$WORK/preg.txt"
  fi
  if docker inspect "$MIRROR" >/dev/null 2>&1; then
    echo "--- mirror-test registry"
    docker logs --tail 25 "$MIRROR" 2>&1 | tee "$WORK/mreg.txt" || true
    annotate "mirror-test registry" < "$WORK/mreg.txt"
  fi
  if docker inspect "$REGISTRY" >/dev/null 2>&1; then
    echo "--- registry"
    docker logs --tail 25 "$REGISTRY" 2>&1 | tee "$WORK/reg.txt" || true
    annotate "registry" < "$WORK/reg.txt"
  fi
  if declare -F extra_diag >/dev/null; then
    echo "--- extra diagnostics"
    extra_diag 2>&1 | tee "$WORK/extra.txt" || true
    annotate "extra diagnostics" < "$WORK/extra.txt"
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
  docker rm -f "$PRIVATE" >/dev/null 2>&1 || true
  docker rm -f "$MIRROR" >/dev/null 2>&1 || true
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

# cluster_metric NAME: the sum of a counter over every node. With coordination a
# blob a node asked for may have been fetched from the registry by another node.
cluster_metric() {
  local n total=0
  for n in "${NODES[@]}"; do total=$(( total + $(metric "$n" "$1") )); done
  echo "$total"
}

# from_cluster NODE: blobs NODE got without the registry, from a peer or from
# its own cache. A blob can already be on a node that coordinated its fetch for
# another one, so a pull there counts a local hit where a peer hit was expected.
from_cluster() { echo $(( $(metric "$1" sic_peer_hits_total) + $(metric "$1" sic_local_hits_total) )); }

# sees_all_peers NODE: whether NODE's SIC lists every other node's SIC. Waiting
# for just one is a race: a node that has only seen some of them asks those, and
# goes to the registry for what only the others hold.
sees_all_peers() {
  local want=$(( $(kubectl get nodes --no-headers | wc -l) - 1 ))
  [ "$(metric "$1" sic_peers)" -ge "$want" ]
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
  both_see_peers() { sees_all_peers "$A" && sees_all_peers "$B"; }
  wait_for "both workers to see all their peers" 90 both_see_peers

  # Public registries throttle anonymous clients by address, and the runner's is
  # shared. A pull that fails is tried again after a pause. If the last attempt
  # was refused because the registry is rate limiting (SIC passes its 429 on to
  # containerd, so the message says so), that registry is skipped and the others
  # still run; anything else is a failure. what is checked is SIC's behaviour on
  # the attempt that works.
  is_throttle() { grep -qiE '429|too many requests|toomanyrequests|rate limit' <<<"$1"; }
  # pull_retry NODE IMAGE: 0 pulled, 2 the registry kept throttling, else fail.
  pull_retry() {
    local node=$1 img=$2 try out
    for try in 1 2 3 4; do
      if out=$(docker exec "$node" crictl pull "$img" 2>&1); then echo "$out"; return 0; fi
      echo "$out"
      echo "pull of $img on $node failed (attempt $try of 4); waiting before trying again"
      [ "$try" = 4 ] || sleep $((try * 20))
    done
    if is_throttle "$out"; then return 2; fi
    fail "$img could not be pulled on $node after 4 attempts"
  }

  # check_image IMAGE: A pulls it from the registry, then B pulls it from A.
  # Returns 2 if the registry throttled before both had been checked.
  check_image() {
    local img=$1 rc ch_before up_before peer_before
    log "registries: $img on $A, then on $B"
    ch_before=$(metric "$A" sic_upstream_auth_challenges_total)
    up_before=$(cluster_metric sic_upstream_hits_total)
    pull_retry "$A" "$img" && rc=0 || rc=$?
    [ "$rc" = 0 ] || return "$rc"
    echo "challenges relayed on $A: $ch_before -> $(metric "$A" sic_upstream_auth_challenges_total)"
    echo "upstream fetches in the cluster: $up_before -> $(cluster_metric sic_upstream_hits_total)"
    [ "$(metric "$A" sic_upstream_auth_challenges_total)" -gt "$ch_before" ] || fail "$img: no authentication challenge was relayed on $A"
    [ $(( $(cluster_metric sic_upstream_hits_total) - up_before )) -ge 2 ] || fail "$img: fewer than 2 objects were fetched from the registry for $A"

    peer_before=$(from_cluster "$B")
    pull_retry "$B" "$img" && rc=0 || rc=$?
    [ "$rc" = 0 ] || return "$rc"
    echo "peer or local hits on $B: $peer_before -> $(from_cluster "$B")"
    [ $(( $(from_cluster "$B") - peer_before )) -ge 2 ] || fail "$img: $B got fewer than 2 blobs from its peer"
  }

  local img rc checked=() skipped=()
  for img in public.ecr.aws/docker/library/busybox:latest ghcr.io/jqlang/jq:latest; do
    check_image "$img" && rc=0 || rc=$?
    if [ "$rc" = 0 ]; then checked+=("$img"); else skipped+=("$img"); fi
  done
  [ "$(metric "$B" sic_verify_errors_total)" = 0 ] || fail "$B had verification errors"
  [ "$(metric "$B" sic_peer_errors_total)" = 0 ] || fail "$B had peer errors"
  if [ "${#skipped[@]}" -gt 0 ]; then
    echo "::warning::skipped, because the registry was rate limiting: ${skipped[*]}"
    log "SKIPPED (throttled by the registry, nothing to conclude about SIC): ${skipped[*]}"
  fi
  [ "${#checked[@]}" -gt 0 ] || fail "every registry was rate limiting; nothing was checked (${skipped[*]})"
  log "pass: ${checked[*]} over the registry's own token service, then shared between nodes"
}

log "build the SIC image"
docker build --build-arg VERSION=e2e -t "$SIC_IMAGE" "$ROOT"

log "create the kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --config "$ROOT/test/e2e/kind-config.yaml" --kubeconfig "$KUBECONFIG" --wait 120s
mapfile -t NODES < <(kind get nodes --name "$CLUSTER")
A=$CLUSTER-worker
B=$CLUSTER-worker2
for n in "$A" "$B"; do [[ " ${NODES[*]} " == *" $n "* ]] || fail "expected node $n, have: ${NODES[*]}"; done
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
# Not "ctr ... | grep -q": with pipefail, grep exiting at the first match makes
# ctr die of SIGPIPE and the pipeline report failure, depending on timing.
images_on_b=$(docker exec "$B" ctr -n k8s.io images ls -q)
if grep -q "$APP" <<<"$images_on_b"; then fail "node $B should not have the image yet"; fi

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
sees_peer() { sees_all_peers "$B"; }
wait_for "$B's SIC to see all its peers" 90 sees_peer
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

images_on_b=$(docker exec "$B" ctr -n k8s.io images ls -q)
grep -q "$APP@$DIGEST" <<<"$images_on_b" || fail "$B does not have the image after the pull"
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

USER_NAME=e2euser
PASS=$(openssl rand -hex 12)
# start_private_registry: the registry that wants a password. It has to be
# running, and its name known to the nodes, before SIC starts: a pod on the host
# network gets a copy of the node's /etc/hosts when it is created, so an entry
# added later is not seen by it.
start_private_registry() {
  for img in "${HTTPD_IMAGES[@]}"; do
    if docker pull -q "$img" >/dev/null 2>&1; then HTTPD_IMAGE=$img; break; fi
  done
  [ -n "${HTTPD_IMAGE:-}" ] || fail "could not pull an httpd image for htpasswd (tried: ${HTTPD_IMAGES[*]})"
  mkdir "$WORK/auth"
  # The registry accepts bcrypt only.
  docker run --rm --entrypoint htpasswd "$HTTPD_IMAGE" -Bbn "$USER_NAME" "$PASS" > "$WORK/auth/htpasswd"
  (
    cd "$WORK"
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$PRIVATE_HOST" -keyout preg.key -out preg.csr 2>/dev/null
    openssl x509 -req -in preg.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -out preg.crt -extfile <(printf '%s\n' \
      "subjectAltName=DNS:$PRIVATE_HOST" 'extendedKeyUsage=serverAuth' 'keyUsage=critical,digitalSignature') 2>/dev/null
  )
  docker rm -f "$PRIVATE" >/dev/null 2>&1 || true
  docker run -d --name "$PRIVATE" --network kind -p 127.0.0.1:5444:443 -v "$WORK:/certs:ro" -v "$WORK/auth:/auth:ro" \
    -e REGISTRY_HTTP_ADDR=0.0.0.0:443 -e REGISTRY_HTTP_TLS_CERTIFICATE=/certs/preg.crt -e REGISTRY_HTTP_TLS_KEY=/certs/preg.key \
    -e REGISTRY_AUTH=htpasswd -e REGISTRY_AUTH_HTPASSWD_REALM=e2e -e REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd \
    "$REGISTRY_IMAGE" >/dev/null
  # 401 without credentials is what a working registry of this kind answers.
  private_up() { [ "$(curl -s -o /dev/null -w '%{http_code}' --cacert "$WORK/ca.crt" --resolve "$PRIVATE_HOST:5444:127.0.0.1" "https://$PRIVATE_HOST:5444/v2/")" = 401 ]; }
  wait_for "the private registry" 60 private_up
  PRIVATE_IP=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$PRIVATE")
  for n in "${NODES[@]}"; do docker exec "$n" sh -c "echo '$PRIVATE_IP $PRIVATE_HOST' >> /etc/hosts"; done
}

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
start_private_registry

# push NAME [TAG]: a new image with random layers under $LOCAL:TAG (default v1);
# prints its digest.
# The docker daemon treats localhost registries as insecure, so it does not
# need the test CA.
push() {
  local dir=$WORK/up-$1
  mkdir -p "$dir"
  head -c 3000000 /dev/urandom > "$dir/one"
  head -c 2000000 /dev/urandom > "$dir/two"
  printf 'FROM scratch\nCOPY one /one\nCOPY two /two\n' > "$dir/Dockerfile"
  docker build -q -t "$LOCAL:${2:-v1}" "$dir" >/dev/null
  docker push "$LOCAL:${2:-v1}" | awk '/digest: sha256:/ {print $3}'
}
registry_blob_gets() { docker logs "$REGISTRY" 2>&1 | grep -c 'GET /v2/e2e/up/blobs/sha256:' || true; }
node_digest() { docker exec "$1" ctr -n k8s.io images ls "name==$UP:v1" | awk 'NR==2 {print $3}'; }

log "point SIC at the registry: upstream on, private CA through SSL_CERT_FILE, short tag TTL"
kubectl -n kube-system create configmap upstream-ca --from-file=ca-bundle.crt="$WORK/ca.crt"
kubectl -n kube-system patch daemonset steady-image-cache --type=json -p '[
  {"op":"add","path":"/spec/template/spec/volumes/-","value":{"name":"upstream-ca","configMap":{"name":"upstream-ca"}}},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"upstream-ca","mountPath":"/etc/sic-roots","readOnly":true}}]'
kubectl -n kube-system set env daemonset/steady-image-cache \
  SIC_UPSTREAM_ENABLED=true SIC_UPSTREAM_REGISTRIES="$REGISTRY_HOST,$PRIVATE_HOST" SSL_CERT_FILE=/etc/sic-roots/ca-bundle.crt SIC_TAG_TTL=3s
kubectl -n kube-system rollout status daemonset/steady-image-cache --timeout=180s
both_see_peers() { sees_all_peers "$A" && sees_all_peers "$B"; }
wait_for "both workers to see all their peers" 90 both_see_peers

D1=$(push first)
[[ $D1 == sha256:* ]] || fail "could not push the first image (digest '$D1')"
echo "pushed $UP:v1 = $D1"

log "pull the tag on $A: from the registry, through SIC, over the private CA"
docker exec "$A" crictl pull "$UP:v1"
[ "$(node_digest "$A")" = "$D1" ] || fail "$A has $(node_digest "$A") for the tag, want $D1"
# A blob may have been fetched by the node that coordinates it, so count the cluster.
[ "$(cluster_metric sic_upstream_hits_total)" -ge 3 ] || fail "the cluster made $(cluster_metric sic_upstream_hits_total) upstream fetches for $A's pull, want at least 3 (config and two layers)"
gets_after_a=$(registry_blob_gets)
[ "$gets_after_a" -ge 3 ] || fail "the registry's log shows $gets_after_a blob requests after $A's pull, want at least 3 (is the log format as expected?)"

log "pull the same tag on $B: blobs from $A, none from the registry"
peer_before=$(from_cluster "$B")
docker exec "$B" crictl pull "$UP:v1"
[ "$(node_digest "$B")" = "$D1" ] || fail "$B has $(node_digest "$B") for the tag, want $D1"
[ "$(registry_blob_gets)" = "$gets_after_a" ] || fail "the registry served blobs again for $B ($gets_after_a -> $(registry_blob_gets))"
peer_gain=$(( $(from_cluster "$B") - peer_before ))
[ "$peer_gain" -ge 3 ] || fail "$B had $peer_gain peer hits, want at least 3"

log "the tag moves: a node must not keep serving the old digest"
D2=$(push second)
[ "$D2" != "$D1" ] || fail "the second image has the first one's digest"
sleep 4  # longer than SIC_TAG_TTL
docker exec "$A" crictl pull "$UP:v1"
[ "$(node_digest "$A")" = "$D2" ] || fail "$A still has $(node_digest "$A") for the moved tag, want $D2"
[ "$(metric "$A" sic_tag_changes_total)" -ge 1 ] || fail "$A did not count a tag change"

log "every node pulls a new image at the same moment: the registry serves each blob once"
push herd herd >/dev/null
herd_before=$(registry_blob_gets)
coord_before=$(cluster_metric sic_coordinator_served_total)
pids=()
for n in "$A" "$B" "$CLUSTER-control-plane"; do
  docker exec "$n" crictl pull "$UP:herd" >"$WORK/herd-$n.log" 2>&1 &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid" || { cat "$WORK"/herd-*.log; fail "a pull of $UP:herd failed"; }; done
herd_gets=$(( $(registry_blob_gets) - herd_before ))
echo "registry blob requests for three simultaneous pulls: $herd_gets (config and two layers = 3)"
echo "coordinator requests served: $coord_before -> $(cluster_metric sic_coordinator_served_total)"
[ "$herd_gets" = 3 ] || fail "the registry served $herd_gets blobs for three simultaneous pulls, want 3, one per blob"
[ "$(cluster_metric sic_coordinator_served_total)" -gt "$coord_before" ] || fail "no node answered as a coordinator"

log "the registry goes away: a cached tag is still served, and counted as stale"
docker stop "$REGISTRY" >/dev/null
sleep 4
stale_before=$(metric "$B" sic_stale_tags_served_total)
docker exec "$B" crictl pull "$UP:v1"
[ "$(node_digest "$B")" = "$D1" ] || fail "$B has $(node_digest "$B") after the registry went away, want its cached $D1"
[ "$(metric "$B" sic_stale_tags_served_total)" -gt "$stale_before" ] || fail "$B did not count a stale tag served"

log "phase 2 passed: peers, upstream over a private CA, a moving tag, and a registry that is gone"

# ---------------------------------------------------------------------------
# Phase 3: a registry that wants credentials (HTTP Basic, from htpasswd).
#
# Covers what SIC does with the Authorization header: nothing is cached for a
# request that was refused, credentials are passed on and never kept, and the
# blobs fetched with them are shared with the next node. It also pins what the
# README says about who may read the cache: it does not check who is asking.
# ---------------------------------------------------------------------------
PRIV=$PRIVATE_HOST/e2e/priv
PRIV_LOCAL=localhost:5444/e2e/priv
C=$CLUSTER-control-plane

log "phase 3: a registry with a password"
all_see_peers() { for n in "$A" "$B" "$C"; do sees_all_peers "$n" || return 1; done; }
wait_for "all three nodes to see all their peers" 90 all_see_peers
# Discovery follows the pods with a watch on a real API server. It logs when a
# listing or the watch fails, and when the watch is refused and it falls back to
# listing; none of that may have happened.
discovery_log=$(kubectl -n kube-system logs -l app=steady-image-cache --tail=-1 --prefix 2>&1 | grep 'peer discovery' || true)
[ -z "$discovery_log" ] || fail "peer discovery logged problems (is the watch refused?): $discovery_log"

# A docker config of its own, so that nothing is written to the user's.
export DOCKER_CONFIG=$WORK/dockercfg
mkdir -p "$DOCKER_CONFIG"
printf '%s' "$PASS" | docker login localhost:5444 -u "$USER_NAME" --password-stdin >/dev/null
head -c 3000000 /dev/urandom > "$WORK/auth/one"
head -c 2000000 /dev/urandom > "$WORK/auth/two"
printf 'FROM scratch\nCOPY one /one\nCOPY two /two\n' > "$WORK/auth/Dockerfile"
docker build -q -t "$PRIV_LOCAL:v1" "$WORK/auth" >/dev/null
DP=$(docker push "$PRIV_LOCAL:v1" | awk '/digest: sha256:/ {print $3}')
[[ $DP == sha256:* ]] || fail "could not push to the private registry (digest '$DP')"
echo "pushed $PRIV:v1 = $DP"
unset DOCKER_CONFIG

blob_gets() { docker logs "$PRIVATE" 2>&1 | grep -c 'GET /v2/e2e/priv/blobs/sha256:' || true; }
priv_digest() { docker exec "$1" ctr -n k8s.io images ls "name==$PRIV:v1" | awk 'NR==2 {print $3}'; }
# refused WHAT CMD...: the command must fail, and the registry must not have
# served a blob since GETS_BASE (none at all, until the first real pull).
GETS_BASE=0
refused() {
  local what=$1; shift
  if "$@" >/dev/null 2>&1; then fail "$what: the pull succeeded, it should have been refused"; fi
  [ "$(blob_gets)" = "$GETS_BASE" ] || fail "$what: the registry served blobs ($GETS_BASE -> $(blob_gets))"
}

log "no credentials, wrong credentials: refused, and nothing is fetched"
refused "no credentials on $A" docker exec "$A" crictl pull "$PRIV:v1"
refused "wrong password on $A" docker exec "$A" crictl pull --creds "$USER_NAME:not-$PASS" "$PRIV:v1"
[ -z "$(priv_digest "$A")" ] || fail "$A has the image after refused pulls"

log "right credentials on $A"
ch_before=$(metric "$A" sic_upstream_auth_challenges_total)
docker exec "$A" crictl pull --creds "$USER_NAME:$PASS" "$PRIV:v1"
[ "$(priv_digest "$A")" = "$DP" ] || fail "$A has '$(priv_digest "$A")' for the tag, want $DP"
[ "$(metric "$A" sic_upstream_auth_challenges_total)" -gt "$ch_before" ] || fail "$A relayed no challenge (did the registry ask for credentials?)"
[ "$(cluster_metric sic_upstream_hits_total)" -ge 3 ] || fail "the cluster fetched $(cluster_metric sic_upstream_hits_total) objects from the registry, want at least 3"
gets=$(blob_gets)
[ "$gets" -ge 3 ] || fail "the private registry's log shows $gets blob requests, want at least 3 (is the log format as expected?)"
GETS_BASE=$gets

log "the password is not kept: not in SIC's cache directory, not in its logs"
b64=$(printf '%s:%s' "$USER_NAME" "$PASS" | base64 -w0)
scan() { docker exec "$1" grep -rqF -e "$PASS" -e "$b64" /var/lib/steady-image-cache; }
for n in "${NODES[@]}"; do
  # First show that the scan can see: with the password planted it must find it.
  docker exec "$n" sh -c "printf '%s' '$PASS' > /var/lib/steady-image-cache/canary"
  rc=0; scan "$n" || rc=$?
  docker exec "$n" rm -f /var/lib/steady-image-cache/canary
  [ "$rc" = 0 ] || fail "the scan of $n's cache directory did not find a planted password (grep status $rc)"
  rc=0; scan "$n" || rc=$?
  [ "$rc" = 1 ] || fail "the password (or its Basic form) was found in $n's cache directory (grep status $rc; 1 means not found)"
done
logs=$(kubectl -n kube-system logs -l app=steady-image-cache --tail=-1 --prefix 2>&1)
if grep -qF -e "$PASS" -e "$b64" <<<"$logs"; then fail "the password appears in SIC's logs"; fi

log "right credentials on $B: the blobs come from $A, not the registry"
peer_before=$(from_cluster "$B")
docker exec "$B" crictl pull --creds "$USER_NAME:$PASS" "$PRIV:v1"
[ "$(priv_digest "$B")" = "$DP" ] || fail "$B has '$(priv_digest "$B")' for the tag, want $DP"
[ "$(blob_gets)" = "$gets" ] || fail "the registry served blobs again for $B ($gets -> $(blob_gets))"
[ $(( $(from_cluster "$B") - peer_before )) -ge 3 ] || fail "$B had fewer than 3 peer hits"

log "the documented boundary: the cache does not check who is asking"
# README, "Upstream fallback": once something has been fetched with credentials,
# any client the listener accepts can read it from this node. A tag is only
# confirmed when it is older than SIC_TAG_TTL, and a registry that answers the
# confirmation with a challenge does not stop the cached tag being served.
sleep 4
stale_before=$(metric "$A" sic_stale_tags_served_total)
docker exec "$A" crictl pull "$PRIV:v1"   # no credentials at all
[ "$(metric "$A" sic_stale_tags_served_total)" -gt "$stale_before" ] || fail "$A did not serve the cached tag to a client without credentials"
[ "$(blob_gets)" = "$gets" ] || fail "the registry served blobs for a client without credentials"
# A node that has fetched nothing has nothing to serve by tag...
refused "no credentials on $C, by tag" docker exec "$C" crictl pull "$PRIV:v1"
# ...but a digest is a capability: a peer that has it hands it over.
docker exec "$C" crictl pull "$PRIV@$DP"
[ "$(blob_gets)" = "$gets" ] || fail "the registry served blobs for $C, which asked a peer"
[ "$(from_cluster "$C")" -ge 3 ] || fail "$C had fewer than 3 peer hits by digest"

log "phase 3 passed: credentials passed on and never kept, shared between nodes, and the boundary is as documented"

# ---------------------------------------------------------------------------
# Phase 4: SIC as a mirror (deploy/containerd/mirror-hosts.toml).
#
# The registry here is one SIC is NOT allowed to contact (it is not in
# SIC_UPSTREAM_REGISTRIES), as with the sample DaemonSet's upstream fallback off.
# containerd gets the shipped sample, with only the registry's name and its CA
# filled in, so that a change to that file that breaks it fails here:
#
#   - SIC does not have the content: containerd goes on to the registry. What it
#     fetched there reaches SIC through the containerd import, and the next node
#     gets it from its peer, not from the registry.
#   - SIC is not reachable (connection refused, as with its pod gone): the pull
#     still works, straight from the registry.
#   - A tag that moved: with "pull" only, containerd asks the registry what the
#     tag names. With "resolve" added and no way for SIC to check a tag, a node
#     is handed the old digest by a peer's cached alias; this pins that, because
#     the sample's comment and the README say so.
# ---------------------------------------------------------------------------
M=$MIRROR_HOST/e2e/mirror
LOCAL_M=localhost:5445/e2e/mirror

log "phase 4: a registry SIC may not contact, behind the sample mirror configuration"
(
  cd "$WORK"
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$MIRROR_HOST" -keyout mreg.key -out mreg.csr 2>/dev/null
  openssl x509 -req -in mreg.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -out mreg.crt -extfile <(printf '%s\n' \
    "subjectAltName=DNS:$MIRROR_HOST" 'extendedKeyUsage=serverAuth' 'keyUsage=critical,digitalSignature') 2>/dev/null
)
docker rm -f "$MIRROR" >/dev/null 2>&1 || true
docker run -d --name "$MIRROR" --network kind -p 127.0.0.1:5445:443 -v "$WORK:/certs:ro" \
  -e REGISTRY_HTTP_ADDR=0.0.0.0:443 -e REGISTRY_HTTP_TLS_CERTIFICATE=/certs/mreg.crt -e REGISTRY_HTTP_TLS_KEY=/certs/mreg.key \
  "$REGISTRY_IMAGE" >/dev/null
mirror_up() { curl -fsS -o /dev/null --cacert "$WORK/ca.crt" --resolve "$MIRROR_HOST:5445:127.0.0.1" "https://$MIRROR_HOST:5445/v2/"; }
wait_for "the mirror-test registry" 60 mirror_up
MIRROR_IP=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$MIRROR")
[ -n "$MIRROR_IP" ] || fail "no address for the mirror-test registry container"
# Only containerd on the nodes needs the name: SIC never contacts this registry.
for n in "${NODES[@]}"; do docker exec "$n" sh -c "echo '$MIRROR_IP $MIRROR_HOST' >> /etc/hosts"; done

# The shipped sample, for this registry: its server, and the root-level "ca" the
# sample shows commented out (the registry's certificate is from the test CA,
# which the node does not trust unless told to).
sed -e "s|^server = .*|server = \"https://$MIRROR_HOST\"|" \
    -e 's|^# ca = .*|ca = "/etc/steady-image-cache/ca.crt"|' \
    "$ROOT/deploy/containerd/mirror-hosts.toml" > "$WORK/mirror-hosts.toml"
grep -q '^server = "https://'"$MIRROR_HOST"'"$' "$WORK/mirror-hosts.toml" || fail "mirror-hosts.toml has no server line to point at the test registry"
grep -q '^ca = "/etc/steady-image-cache/ca.crt"$' "$WORK/mirror-hosts.toml" || fail "mirror-hosts.toml has no commented-out root-level ca line (the fallback registry's trust)"
grep -q '^ *capabilities = \["pull"\]$' "$WORK/mirror-hosts.toml" || fail "mirror-hosts.toml must advertise exactly [\"pull\"]: see its comment on tags"
# With "resolve" added, for the control further down.
sed 's|capabilities = \["pull"\]|capabilities = ["pull", "resolve"]|' "$WORK/mirror-hosts.toml" > "$WORK/mirror-hosts-resolve.toml"
install_mirror_toml() { # NODE FILE
  docker exec "$1" mkdir -p "/etc/containerd/certs.d/$MIRROR_HOST"
  docker cp "$2" "$1:/etc/containerd/certs.d/$MIRROR_HOST/hosts.toml"
}
for n in "${NODES[@]}"; do install_mirror_toml "$n" "$WORK/mirror-hosts.toml"; done

# push_m TAG: a new image with random layers under $LOCAL_M:TAG; prints its digest.
push_m() {
  local dir
  dir=$(mktemp -d "$WORK/m-XXXXXX")
  head -c 3000000 /dev/urandom > "$dir/one"
  head -c 2000000 /dev/urandom > "$dir/two"
  printf 'FROM scratch\nCOPY one /one\nCOPY two /two\n' > "$dir/Dockerfile"
  docker build -q -t "$LOCAL_M:$1" "$dir" >/dev/null
  docker push "$LOCAL_M:$1" | awk '/digest: sha256:/ {print $3}'
}
mirror_blob_gets() { docker logs "$MIRROR" 2>&1 | grep -c 'GET /v2/e2e/mirror/blobs/sha256:' || true; }
m_digest() { docker exec "$1" ctr -n k8s.io images ls "name==$M:$2" | awk 'NR==2 {print $3}'; }
# sic_serves NODE REF: NODE's SIC answers for the manifest, over mutual TLS as containerd would.
sic_serves() {
  docker exec "$1" curl -fsS -o /dev/null -I --cacert /etc/steady-image-cache/ca.crt \
    --cert /etc/steady-image-cache/tls.crt --key /etc/steady-image-cache/tls.key \
    "https://127.0.0.1:5000/v2/e2e/mirror/manifests/$2?ns=$MIRROR_HOST"
}

# sic_holds_all NODE MANIFEST_DIGEST: NODE's SIC serves the manifest and every
# blob it names (config and layers). The import copies them one by one, so having
# the manifest says nothing about the layers: a node that asks in between is
# answered "not here" for those, and containerd then takes them from the registry.
sic_holds_all() {
  local manifest ids d
  manifest=$(docker exec "$1" curl -fsS --cacert /etc/steady-image-cache/ca.crt \
    --cert /etc/steady-image-cache/tls.crt --key /etc/steady-image-cache/tls.key \
    "https://127.0.0.1:5000/v2/e2e/mirror/manifests/$2?ns=$MIRROR_HOST") || return 1
  ids=$(grep -o 'sha256:[0-9a-f]\{64\}' <<<"$manifest" | sort -u)
  [ -n "$ids" ] || return 1
  for d in $ids; do
    docker exec "$1" curl -fsS -o /dev/null -I --cacert /etc/steady-image-cache/ca.crt \
      --cert /etc/steady-image-cache/tls.crt --key /etc/steady-image-cache/tls.key \
      "https://127.0.0.1:5000/v2/e2e/mirror/blobs/$d?ns=$MIRROR_HOST" || return 1
  done
}

# What a failed wait for the import should show: where each blob of the image is,
# on node $A, in SIC (by an HTTP HEAD, as containerd would ask), in SIC's cache
# directory and in containerd's content store, and what the import counted.
extra_diag() {
  local mf ids d h
  echo "import on $A: blobs=$(metric "$A" sic_containerd_imported_blobs_total) errors=$(metric "$A" sic_containerd_import_errors_total) store_errors=$(metric "$A" sic_store_errors_total) disk_full=$(metric "$A" sic_disk_full_total)"
  mf=$(docker exec "$A" curl -sS --cacert /etc/steady-image-cache/ca.crt --cert /etc/steady-image-cache/tls.crt --key /etc/steady-image-cache/tls.key \
    "https://127.0.0.1:5000/v2/e2e/mirror/manifests/${DM1:-none}?ns=$MIRROR_HOST" 2>&1) || true
  ids=$(grep -o 'sha256:[0-9a-f]\{64\}' <<<"$mf" | sort -u)
  [ -n "$ids" ] || echo "SIC on $A did not return a manifest with digests: ${mf:0:200}"
  for d in $ids; do
    h=$(docker exec "$A" curl -s -o /dev/null -w '%{http_code}' -I --cacert /etc/steady-image-cache/ca.crt --cert /etc/steady-image-cache/tls.crt --key /etc/steady-image-cache/tls.key \
      "https://127.0.0.1:5000/v2/e2e/mirror/blobs/$d?ns=$MIRROR_HOST" 2>&1)
    echo "$d: SIC HEAD $h; in SIC's cache: $(docker exec "$A" sh -c "ls -l /var/lib/steady-image-cache/blobs/sha256/${d#sha256:} 2>&1 | awk '{print \$5, \$NF}'"); containerd: $(docker exec "$A" ctr -n k8s.io content ls 2>&1 | grep -c "${d#sha256:}" || true) listing(s)"
  done
  echo "free space on $A: $(docker exec "$A" df -h /var/lib/steady-image-cache 2>&1 | tail -1)"
}

wait_for "all three nodes to see all their peers" 90 all_see_peers
DM1=$(push_m v1)
[[ $DM1 == sha256:* ]] || fail "could not push to the mirror-test registry (digest '$DM1')"
echo "pushed $M:v1 = $DM1"

log "SIC does not have it: containerd goes on to the registry, over the registry's own CA"
miss_before=$(metric "$A" sic_misses_total)
docker exec "$A" crictl pull "$M:v1"
[ "$(m_digest "$A" v1)" = "$DM1" ] || fail "$A has '$(m_digest "$A" v1)' for the tag, want $DM1"
[ "$(metric "$A" sic_misses_total)" -gt "$miss_before" ] || fail "$A's SIC saw no miss: did containerd ask it before the registry?"
gets_a=$(mirror_blob_gets)
[ "$gets_a" -ge 3 ] || fail "the registry's log shows $gets_a blob requests after $A's pull, want at least 3 (config and two layers)"

log "what $A fetched there reaches SIC through the import; $B gets it from $A, not from the registry"
wait_for "$A's SIC to hold the manifest and all its blobs" 90 sic_holds_all "$A" "$DM1"
wait_for "$A's SIC to serve the tag (an alias imported from containerd)" 90 sic_serves "$A" v1
peer_before=$(from_cluster "$B")
docker exec "$B" crictl pull "$M:v1"
[ "$(m_digest "$B" v1)" = "$DM1" ] || fail "$B has '$(m_digest "$B" v1)' for the tag, want $DM1"
[ "$(mirror_blob_gets)" = "$gets_a" ] || fail "the registry served blobs again for $B ($gets_a -> $(mirror_blob_gets))"
peer_gain=$(( $(from_cluster "$B") - peer_before ))
[ "$peer_gain" -ge 3 ] || fail "$B had $peer_gain peer hits, want at least 3"

log "the tag moves, and SIC cannot check tags for this registry"
DM2=$(push_m v1)
[ "$DM2" != "$DM1" ] || fail "the second image has the first one's digest"
# $C has never had this tag. A peer's alias says it is $DM1, imported from containerd.
install_mirror_toml "$C" "$WORK/mirror-hosts-resolve.toml"
docker exec "$C" crictl pull "$M:v1"
[ "$(m_digest "$C" v1)" = "$DM1" ] || fail "with resolve, $C has '$(m_digest "$C" v1)' for the moved tag; the documented hazard is that it gets the old $DM1"
echo "control: with resolve advertised, $C was handed the old digest, as the sample's comment says"
install_mirror_toml "$C" "$WORK/mirror-hosts.toml"
gets_before=$(mirror_blob_gets)
docker exec "$C" crictl pull "$M:v1"
[ "$(m_digest "$C" v1)" = "$DM2" ] || fail "with pull only, $C has '$(m_digest "$C" v1)' for the moved tag, want $DM2"
gets_after=$(mirror_blob_gets)
[ $((gets_after - gets_before)) -ge 3 ] || fail "the new image's blobs did not come from the registry ($gets_before -> $gets_after)"

log "SIC is not reachable: the pull still works, straight from the registry"
docker exec "$B" iptables --version >/dev/null 2>&1 || fail "no iptables on $B to make SIC unreachable"
DM3=$(push_m down)
[[ $DM3 == sha256:* ]] || fail "could not push the third image (digest '$DM3')"
sic_reachable() { docker exec "$1" bash -c 'exec 3<>/dev/tcp/127.0.0.1/5000' 2>/dev/null; }
sic_reachable "$B" || fail "SIC on $B is not reachable before the test"
# What containerd sees when SIC's pod is gone: the connection is refused. Only
# the node's own loopback traffic to SIC's port is affected, not its peers'.
docker exec "$B" iptables -I INPUT -p tcp -d 127.0.0.1 --dport 5000 -j REJECT --reject-with tcp-reset
if sic_reachable "$B"; then fail "SIC on $B is still reachable after blocking its port"; fi
seen_before=$(( $(metric "$B" sic_misses_total) + $(metric "$B" sic_local_hits_total) + $(metric "$B" sic_peer_hits_total) ))
gets_before=$(mirror_blob_gets)
rc=0
docker exec "$B" crictl pull "$M:down" || rc=$?
docker exec "$B" iptables -D INPUT -p tcp -d 127.0.0.1 --dport 5000 -j REJECT --reject-with tcp-reset
[ "$rc" = 0 ] || fail "the pull on $B failed while SIC was unreachable"
[ "$(m_digest "$B" down)" = "$DM3" ] || fail "$B has '$(m_digest "$B" down)' for the tag, want $DM3"
gets_after=$(mirror_blob_gets)
[ $((gets_after - gets_before)) -ge 3 ] || fail "the blobs did not come from the registry ($gets_before -> $gets_after)"
seen_after=$(( $(metric "$B" sic_misses_total) + $(metric "$B" sic_local_hits_total) + $(metric "$B" sic_peer_hits_total) ))
[ "$seen_after" = "$seen_before" ] || fail "SIC on $B handled requests while its port was blocked ($seen_before -> $seen_after)"
wait_for "SIC on $B to be reachable again" 10 sic_reachable "$B"

log "phase 4 passed: the sample mirror falls back when SIC has nothing or is gone, shares what it fetched, and follows a moved tag"
