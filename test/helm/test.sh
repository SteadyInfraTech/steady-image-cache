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
  --set serviceMonitor.enabled=true \
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

echo "=== Helm chart tests passed successfully! ==="
