#!/usr/bin/env bash
# Checks the Prometheus side of the project with promtool: the rules (both the
# copy in deploy/monitoring and the chart's PrometheusRule), their unit tests,
# and that every expression of the Grafana dashboard parses.
#
# Needs docker (promtool comes from the Prometheus image), helm and python3 with
# PyYAML. Run from anywhere: test/monitoring/test.sh
set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROM_IMAGE=${PROM_IMAGE:-prom/prometheus:v2.55.1}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

promtool() { docker run --rm -v "$1:/w" -w /w --entrypoint promtool "$PROM_IMAGE" "${@:2}"; }

# groups SRC DST: the rule groups of a PrometheusRule, as promtool wants them.
groups() {
  python3 - "$1" "$2" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d and d.get("kind") == "PrometheusRule"]
assert len(docs) == 1, "expected one PrometheusRule in %s, found %d" % (sys.argv[1], len(docs))
yaml.safe_dump({"groups": docs[0]["spec"]["groups"]}, open(sys.argv[2], "w"))
PY
}

mkdir "$WORK/deploy" "$WORK/chart" "$WORK/dashboard"
groups "$ROOT/deploy/monitoring/prometheus-rules.yaml" "$WORK/deploy/rules.yaml"
helm template sic "$ROOT/charts/steady-image-cache" --set prometheusRule.enabled=true > "$WORK/chart.yaml"
groups "$WORK/chart.yaml" "$WORK/chart/rules.yaml"

for set in deploy chart; do
  echo "=== $set: promtool check rules"
  promtool "$WORK/$set" check rules rules.yaml
  echo "=== $set: promtool test rules"
  cp "$ROOT/test/monitoring/alerts_test.yaml" "$WORK/$set/alerts_test.yaml"
  promtool "$WORK/$set" test rules alerts_test.yaml
done

echo "=== dashboard: every expression parses"
python3 - "$ROOT/deploy/monitoring/grafana-dashboard.json" "$WORK/dashboard/rules.yaml" <<'PY'
import json, sys, yaml
exprs = []
def walk(o):
    if isinstance(o, dict):
        if isinstance(o.get("expr"), str):
            exprs.append(o["expr"])
        for v in o.values():
            walk(v)
    elif isinstance(o, list):
        for v in o:
            walk(v)
walk(json.load(open(sys.argv[1])))
assert exprs, "no expressions found in the dashboard"
rules = [{"record": "dashboard:expr_%d" % i, "expr": e} for i, e in enumerate(exprs)]
yaml.safe_dump({"groups": [{"name": "dashboard", "rules": rules}]}, open(sys.argv[2], "w"))
print("%d expressions" % len(exprs))
PY
promtool "$WORK/dashboard" check rules rules.yaml

echo "=== dashboard: one data source variable, no fixed data source uid"
python3 - "$ROOT/deploy/monitoring/grafana-dashboard.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert [v["name"] for v in d["templating"]["list"] if v["type"] == "datasource"] == ["datasource"]
bad = []
def walk(o):
    if isinstance(o, dict):
        ds = o.get("datasource")
        if isinstance(ds, dict) and ds.get("uid") != "${datasource}":
            bad.append(ds)
        for v in o.values():
            walk(v)
    elif isinstance(o, list):
        for v in o:
            walk(v)
walk(d["panels"])
assert not bad, "panels with a fixed data source: %s" % bad
PY
echo "monitoring checks passed"
