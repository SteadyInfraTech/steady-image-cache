#!/usr/bin/env bash
# End-to-end test of the Helm chart on kind: a real containerd, cert-manager,
# and SIC installed with `helm install`, nothing from deploy/daemonset.yaml.
#
# What this covers that test/helm/test.sh (templating only) and test/e2e/run.sh
# (the sample DaemonSet, certificates made by hand) cannot:
#
#   * the chart installs, and its pods become Ready on the probes it renders;
#   * `sic init-node` puts containerd's client certificate and hosts.toml on every
#     node, and that certificate is a client-only one that does not name SIC;
#   * a pull on one node is served from another node's cache through the
#     mirror that init-node configured, over mutual TLS with cert-manager's
#     certificates;
#   * the certificate flow: when cert-manager renews the certificates (here by
#     deleting their Secrets, which makes it issue them again), the node's copy
#     for containerd follows, SIC serves the new server certificate, and a pull
#     afterwards still works.
#
# Needs docker, kind, kubectl, helm, openssl. Run from anywhere:
#   test/e2e/helm.sh            # creates the cluster "sic-helm-e2e" and deletes it
#   KEEP=1 test/e2e/helm.sh     # leave the cluster for inspection
set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CLUSTER=${CLUSTER:-sic-helm-e2e}
KEEP=${KEEP:-}
CERT_MANAGER_VERSION=${CERT_MANAGER_VERSION:-v1.16.2}
IMAGE_REPO=sic-helm-e2e
IMAGE_TAG=e2e
NS=kube-system
RELEASE=sic
APP=registry.example.test/e2e/app          # never resolvable: only the mirror can answer
WORK=$(mktemp -d)
export KUBECONFIG=$WORK/kubeconfig

STAGE=start
log() { STAGE=$*; printf '\n=== %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

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
  { echo "stage: $STAGE"; echo "command: $BASH_COMMAND_FAILED (line $LINE_FAILED)"; tail -n 25 "$OUT"; } | annotate "chart e2e failed"
  kubectl get pods -A -o wide 2>&1 | tail -n 40 | tee "$WORK/pods.txt" || true
  annotate "pods" < "$WORK/pods.txt"
  kubectl -n "$NS" get events --sort-by=.lastTimestamp 2>&1 | tail -n 20 | tee "$WORK/events.txt" || true
  annotate "events" < "$WORK/events.txt"
  kubectl get certificate -A 2>&1 | tee "$WORK/certs.txt" || true
  annotate "certificates" < "$WORK/certs.txt"
  for p in $(kubectl -n "$NS" get pods -l app=steady-image-cache -o name 2>/dev/null); do
    for c in sic sic-init sic-init-refresh; do
      echo "--- $p $c"
      kubectl -n "$NS" logs "$p" -c "$c" --tail=25 2>&1 | tee "$WORK/pod.txt" || true
      annotate "$p $c" < "$WORK/pod.txt"
    done
  done
}

cleanup() {
  rc=$?
  trap - ERR
  if [ $rc -ne 0 ]; then dump; sleep 1; fi
  if [ -z "$KEEP" ]; then kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
  rm -rf "$WORK"
  exit $rc
}
BASH_COMMAND_FAILED=; LINE_FAILED=
trap 'BASH_COMMAND_FAILED=$BASH_COMMAND; LINE_FAILED=$LINENO' ERR
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

sees_all_peers() {
  local want=$(( $(kubectl get nodes --no-headers | wc -l) - 1 ))
  [ "$(metric "$1" sic_peers)" -ge "$want" ]
}

# secret_field SECRET KEY: a Secret's value, decoded.
secret_field() { kubectl -n "$NS" get secret "$1" -o "jsonpath={.data.${2//./\\.}}" | base64 -d; }

# node_file NODE PATH: a file on the node.
node_file() { docker exec "$1" cat "$2"; }

log "build the SIC image"
docker build --build-arg VERSION=e2e -t "$IMAGE_REPO:$IMAGE_TAG" "$ROOT"

log "create the kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --config "$ROOT/test/e2e/kind-config.yaml" --kubeconfig "$KUBECONFIG" --wait 120s
mapfile -t NODES < <(kind get nodes --name "$CLUSTER")
A=$CLUSTER-worker
B=$CLUSTER-worker2
for n in "$A" "$B"; do [[ " ${NODES[*]} " == *" $n "* ]] || fail "expected node $n, have: ${NODES[*]}"; done
kind load docker-image "$IMAGE_REPO:$IMAGE_TAG" --name "$CLUSTER"

log "cert-manager $CERT_MANAGER_VERSION, and a CA issuer"
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml"
for d in cert-manager cert-manager-cainjector cert-manager-webhook; do
  kubectl -n cert-manager rollout status "deployment/$d" --timeout=240s
done
cat > "$WORK/issuers.yaml" <<'YAML'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: e2e-selfsigned}
spec: {selfSigned: {}}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: e2e-ca, namespace: cert-manager}
spec:
  isCA: true
  commonName: e2e-ca
  secretName: e2e-ca
  privateKey: {algorithm: ECDSA, size: 256}
  issuerRef: {name: e2e-selfsigned, kind: ClusterIssuer}
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: e2e-ca}
spec: {ca: {secretName: e2e-ca}}
YAML
# The webhook can take a moment after its rollout before it admits objects.
applied() { kubectl apply -f "$WORK/issuers.yaml"; }
wait_for "cert-manager to accept the issuers" 120 applied
kubectl wait --for=condition=Ready clusterissuer/e2e-ca --timeout=120s

log "helm install: the chart as it is, with the local image and a short refresh"
helm install "$RELEASE" "$ROOT/charts/steady-image-cache" -n "$NS" \
  --set image.repository="$IMAGE_REPO" --set image.tag="$IMAGE_TAG" \
  --set initContainer.image.repository="$IMAGE_REPO" --set initContainer.image.tag="$IMAGE_TAG" \
  --set "initContainer.registries={registry.example.test}" \
  --set initContainer.refreshInterval=10s \
  --set config.importInterval=3s \
  --set tls.certManager.enabled=true \
  --set tls.certManager.issuerRef.name=e2e-ca --set tls.certManager.issuerRef.kind=ClusterIssuer \
  --wait --timeout 6m
DS=$RELEASE-steady-image-cache
kubectl -n "$NS" rollout status "daemonset/$DS" --timeout=180s
SERVER_SECRET=steady-image-cache-tls
CLIENT_SECRET=steady-image-cache-containerd-client

log "what init-node left on every node"
for n in "${NODES[@]}"; do
  for f in ca.crt tls.crt tls.key; do
    docker exec "$n" test -s "/etc/steady-image-cache/$f" || fail "$n: /etc/steady-image-cache/$f is missing"
  done
  modes=$(docker exec "$n" stat -c '%a' /etc/steady-image-cache/ca.crt /etc/steady-image-cache/tls.crt /etc/steady-image-cache/tls.key | tr '\n' ' ')
  [ "$modes" = "644 644 600 " ] || fail "$n: modes of ca.crt tls.crt tls.key are '$modes', want '644 644 600 '"
  toml=$(node_file "$n" /etc/containerd/certs.d/registry.example.test/hosts.toml)
  grep -q 'https://127.0.0.1:5000' <<<"$toml" || fail "$n: hosts.toml does not name the local SIC: $toml"
  grep -q '/etc/steady-image-cache/tls.crt' <<<"$toml" || fail "$n: hosts.toml does not name the client certificate: $toml"
done
echo "files and hosts.toml are in place on ${#NODES[@]} nodes"

log "containerd's certificate is a client-only one that does not name SIC, and is not the pod's key"
node_file "$A" /etc/steady-image-cache/tls.crt > "$WORK/node.crt"
san=$(openssl x509 -in "$WORK/node.crt" -noout -ext subjectAltName 2>&1 || true)
eku=$(openssl x509 -in "$WORK/node.crt" -noout -ext extendedKeyUsage 2>&1)
grep -q 'steady-image-cache' <<<"$san" && fail "containerd's certificate names SIC (it would be taken for a node): $san"
grep -qi 'TLS Web Client Authentication' <<<"$eku" || fail "containerd's certificate has no client auth usage: $eku"
grep -qi 'TLS Web Server Authentication' <<<"$eku" && fail "containerd's certificate also allows server auth: $eku"
[ "$(node_file "$A" /etc/steady-image-cache/tls.key)" = "$(secret_field "$CLIENT_SECRET" tls.key)" ] || fail "the node's key is not the client Secret's"
[ "$(node_file "$A" /etc/steady-image-cache/tls.key)" != "$(secret_field "$SERVER_SECRET" tls.key)" ] || fail "the node holds the pod's private key"

# The image only node A has, pulled on B by digest through SIC (as in run.sh).
make_app() { # make_app TAG: a local image with random layers, loaded into $A only
  local dir=$WORK/app-$1
  mkdir "$dir"
  head -c 2000000 /dev/urandom > "$dir/one"
  head -c 1000000 /dev/urandom > "$dir/two"
  printf 'FROM scratch\nCOPY one /one\nCOPY two /two\n' > "$dir/Dockerfile"
  docker build --provenance=false --sbom=false -q -t "$APP:$1" "$dir" >/dev/null
  kind load docker-image "$APP:$1" --name "$CLUSTER" --nodes "$A"
}
digest_of() { docker exec "$A" ctr -n k8s.io images ls "name==$APP:$1" | awk 'NR==2 {print $3}'; }
# serves DIGEST: A's SIC answers for the manifest, to a client with containerd's certificate.
serves() {
  docker exec "$A" curl -fsS -o /dev/null -I --cacert /etc/steady-image-cache/ca.crt \
    --cert /etc/steady-image-cache/tls.crt --key /etc/steady-image-cache/tls.key \
    "https://127.0.0.1:5000/v2/e2e/app/manifests/$1?ns=registry.example.test"
}
pull_through_sic() { # pull_through_sic TAG
  local d before after
  make_app "$1"
  d=$(digest_of "$1")
  [[ $d == sha256:* ]] || fail "no digest for $APP:$1 on $A (got '$d')"
  wait_for "$A's SIC to serve $1" 90 serves "$d"
  both() { sees_all_peers "$A" && sees_all_peers "$B"; }
  wait_for "both workers to see all their peers" 90 both
  before=$(metric "$B" sic_peer_hits_total)
  docker exec "$B" crictl pull "$APP@$d"
  after=$(metric "$B" sic_peer_hits_total)
  echo "peer hits on $B: $before -> $after"
  [ $((after - before)) -ge 4 ] || fail "$1: expected at least 4 peer hits on $B (manifest, config, 2 layers), got $((after - before))"
  [ "$(metric "$B" sic_upstream_hits_total)" = 0 ] || fail "$B contacted an upstream registry"
  [ "$(metric "$B" sic_verify_errors_total)" = 0 ] || fail "$B had verification errors"
}

log "pull on $B through the mirror init-node configured, from $A's cache"
pull_through_sic v1

log "certificate flow: cert-manager issues both certificates again"
old_client=$(secret_field "$CLIENT_SECRET" tls.crt | openssl x509 -noout -serial)
old_server=$(secret_field "$SERVER_SECRET" tls.crt | openssl x509 -noout -serial)
node_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$A")
presented_serial() { # the serial of the server certificate SIC on A presents
  openssl s_client -connect "$node_ip:5000" </dev/null 2>/dev/null | openssl x509 -noout -serial
}
[ "$(presented_serial)" = "$old_server" ] || fail "SIC on $A presents $(presented_serial), the Secret has $old_server"
kubectl -n "$NS" delete secret "$CLIENT_SECRET" "$SERVER_SECRET"
reissued() {
  # Not as one test over two command substitutions: a Secret that is gone for a
  # moment gives an empty serial, which is "different" from the old one.
  local c s
  c=$(secret_field "$CLIENT_SECRET" tls.crt | openssl x509 -noout -serial) || return 1
  s=$(secret_field "$SERVER_SECRET" tls.crt | openssl x509 -noout -serial) || return 1
  [ -n "$c" ] && [ -n "$s" ] && [ "$c" != "$old_client" ] && [ "$s" != "$old_server" ]
}
wait_for "cert-manager to issue both certificates again" 120 reissued
new_client=$(secret_field "$CLIENT_SECRET" tls.crt | openssl x509 -noout -serial)
new_server=$(secret_field "$SERVER_SECRET" tls.crt | openssl x509 -noout -serial)
echo "client: $old_client -> $new_client"
echo "server: $old_server -> $new_server"

# kubelet puts the new Secret into the pod (up to about a minute), the sidecar
# copies it to the node within its interval, SIC reloads within 10 seconds.
node_follows() {
  local n
  for n in "${NODES[@]}"; do
    [ "$(node_file "$n" /etc/steady-image-cache/tls.crt | openssl x509 -noout -serial)" = "$new_client" ] || return 1
    [ "$(node_file "$n" /etc/steady-image-cache/tls.key)" = "$(secret_field "$CLIENT_SECRET" tls.key)" ] || return 1
  done
}
wait_for "every node's copy of containerd's certificate to follow the renewal" 240 node_follows
echo "every node has the renewed client certificate"
server_follows() { [ "$(presented_serial)" = "$new_server" ]; }
wait_for "SIC on $A to present the renewed server certificate" 240 server_follows
echo "SIC presents the renewed server certificate"
modes=$(docker exec "$A" stat -c '%a' /etc/steady-image-cache/tls.crt /etc/steady-image-cache/tls.key | tr '\n' ' ')
[ "$modes" = "644 600 " ] || fail "modes after the renewal are '$modes'"

log "a pull after the renewal: new certificates on both sides"
pull_through_sic v2
[ "$(metric "$B" sic_verify_errors_total)" = 0 ] || fail "$B had verification errors"

log "chart e2e passed: install, init-node, mirror pull, certificate renewal, and a pull after it"
