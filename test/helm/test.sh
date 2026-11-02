#!/usr/bin/env bash
# Tests the Helm chart: linting, templating across different configurations,
# and syntax validation.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CHART_DIR="$ROOT/charts/steady-image-cache"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

export HELM_CACHE_HOME="$TMP_DIR/cache"
export HELM_CONFIG_HOME="$TMP_DIR/config"
export HELM_DATA_HOME="$TMP_DIR/data"

echo "=== Linting Helm chart ==="
helm lint "$CHART_DIR"

echo "=== Testing template: default values ==="
helm template sic "$CHART_DIR" > "$TMP_DIR/default.yaml"
grep -q "kind: DaemonSet" "$TMP_DIR/default.yaml"
grep -q "name: sic-init" "$TMP_DIR/default.yaml"
grep -q "name: sic" "$TMP_DIR/default.yaml"

echo "=== Testing template: initContainer disabled ==="
helm template sic "$CHART_DIR" --set initContainer.enabled=false > "$TMP_DIR/no-init.yaml"
grep -q "kind: DaemonSet" "$TMP_DIR/no-init.yaml"
if grep -q "name: sic-init" "$TMP_DIR/no-init.yaml"; then
  echo "FAIL: sic-init should not exist when initContainer.enabled=false" >&2
  exit 1
fi

echo "=== Testing template: cert-manager enabled ==="
helm template sic "$CHART_DIR" \
  --set tls.certManager.enabled=true \
  --set tls.certManager.issuerRef.name=my-ca-issuer \
  --set tls.certManager.issuerRef.kind=ClusterIssuer > "$TMP_DIR/cert-manager.yaml"
grep -q "kind: Certificate" "$TMP_DIR/cert-manager.yaml"
grep -q "my-ca-issuer" "$TMP_DIR/cert-manager.yaml"

echo "=== Testing template: ServiceMonitor and PrometheusRule enabled ==="
helm template sic "$CHART_DIR" \
  --set serviceMonitor.enabled=true --set config.adminListen=0.0.0.0:5001 \
  --set prometheusRule.enabled=true > "$TMP_DIR/monitoring.yaml"
grep -q "kind: ServiceMonitor" "$TMP_DIR/monitoring.yaml"
grep -q "kind: PrometheusRule" "$TMP_DIR/monitoring.yaml"
grep -q "SteadyImageCacheDiskFull" "$TMP_DIR/monitoring.yaml"

echo "=== Testing template: upstream and protected registries ==="
helm template sic "$CHART_DIR" \
  --set config.upstreamEnabled=true \
  --set config.upstreamRegistries="{docker.io,ghcr.io}" \
  --set config.protectedRegistries="{ghcr.io}" > "$TMP_DIR/upstream.yaml"
grep -q 'name: SIC_UPSTREAM_ENABLED' "$TMP_DIR/upstream.yaml"
grep -q 'value: "docker.io,ghcr.io"' "$TMP_DIR/upstream.yaml"
grep -q 'value: "ghcr.io"' "$TMP_DIR/upstream.yaml"

echo "=== Testing template: probes follow config.adminListen, settings reach the pod ==="
helm template sic "$CHART_DIR" --set config.adminListen=0.0.0.0:5099 \
  --set config.upstreamTimeout=45s --set config.importInterval=7m > "$TMP_DIR/admin.yaml"
test "$(grep -c 'port: 5099' "$TMP_DIR/admin.yaml")" = 2
! grep -q 'port: 5001' "$TMP_DIR/admin.yaml"
grep -q -A1 'name: SIC_UPSTREAM_TIMEOUT' "$TMP_DIR/admin.yaml"
grep -q 'value: "45s"' "$TMP_DIR/admin.yaml"
grep -q 'value: "7m"' "$TMP_DIR/admin.yaml"

echo "=== Testing template: containerd gets a client-only certificate, never the pod's key ==="
# the init containers mount the client Secret, not the DaemonSet's own
awk '/name: sic-init$/,/name: sic$/' "$TMP_DIR/default.yaml" > "$TMP_DIR/init-part.yaml"
grep -q 'mountPath: /tls-client' "$TMP_DIR/init-part.yaml"
! grep -q 'name: tls$' "$TMP_DIR/init-part.yaml"
grep -q 'name: sic-init-refresh' "$TMP_DIR/default.yaml"
helm template sic "$CHART_DIR" --set initContainer.refreshInterval=0 | { ! grep -q 'name: sic-init-refresh'; }
grep -q 'steady-image-cache-containerd-client' "$TMP_DIR/default.yaml"
grep -q 'steady-image-cache-containerd-client' "$TMP_DIR/cert-manager.yaml"
# that certificate is client-only and does not name the server (which would make containerd a node)
awk '/commonName: .*containerd-client/,0' "$TMP_DIR/cert-manager.yaml" > "$TMP_DIR/client-cert.yaml"
grep -q 'client auth' "$TMP_DIR/client-cert.yaml"
! grep -q 'server auth' "$TMP_DIR/client-cert.yaml"
! grep -q 'dnsNames' "$TMP_DIR/client-cert.yaml"

echo "=== Testing template: ServiceMonitor needs a reachable admin address ==="
if helm template sic "$CHART_DIR" --set serviceMonitor.enabled=true > /dev/null 2>"$TMP_DIR/err.txt"; then
  echo "FAIL: serviceMonitor.enabled with a loopback adminListen must be refused" >&2
  exit 1
fi
grep -q 'adminListen' "$TMP_DIR/err.txt"
helm template sic "$CHART_DIR" --set serviceMonitor.enabled=true --set config.adminListen=0.0.0.0:5001 > "$TMP_DIR/sm.yaml"
grep -q 'kind: Service$' "$TMP_DIR/sm.yaml"
grep -q 'name: metrics' "$TMP_DIR/sm.yaml"

echo "=== Helm chart tests passed successfully! ==="
