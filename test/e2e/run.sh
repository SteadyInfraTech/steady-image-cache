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
set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=${CLUSTER:-sic-e2e}
KEEP=${KEEP:-}
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
  docker rm -f "$PRIVATE" >/dev/null 2>&1 || true
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
  SIC_UPSTREAM_ENABLED=true SIC_UPSTREAM_REGISTRIES="$REGISTRY_HOST,$PRIVATE_HOST" SSL_CERT_FILE=/etc/sic-roots/ca-bundle.crt SIC_TAG_TTL=3s
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
all_see_peers() { for n in "$A" "$B" "$C"; do [ "$(metric "$n" sic_peers)" -ge 1 ] || return 1; done; }
wait_for "all three nodes to see a peer" 90 all_see_peers

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
[ "$(metric "$A" sic_upstream_hits_total)" -ge 3 ] || fail "$A fetched $(metric "$A" sic_upstream_hits_total) objects from the registry, want at least 3"
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
peer_before=$(metric "$B" sic_peer_hits_total)
docker exec "$B" crictl pull --creds "$USER_NAME:$PASS" "$PRIV:v1"
[ "$(priv_digest "$B")" = "$DP" ] || fail "$B has '$(priv_digest "$B")' for the tag, want $DP"
[ "$(blob_gets)" = "$gets" ] || fail "the registry served blobs again for $B ($gets -> $(blob_gets))"
[ $(( $(metric "$B" sic_peer_hits_total) - peer_before )) -ge 3 ] || fail "$B had fewer than 3 peer hits"

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
[ "$(metric "$C" sic_peer_hits_total)" -ge 3 ] || fail "$C had fewer than 3 peer hits by digest"

log "pass: credentials passed on and never kept, shared between nodes, and the boundary is as documented"
