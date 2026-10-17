#!/usr/bin/env python3
"""Checks deploy/daemonset.yaml for the things that have to agree with each
other or with how SIC works, and that a schema check cannot know about.

    python3 deploy/check_manifest.py [path]

Exits non-zero and lists every problem found.
"""
import sys

import yaml

path = sys.argv[1] if len(sys.argv) > 1 else "deploy/daemonset.yaml"
docs = list(yaml.safe_load_all(open(path)))
problems = []


def check(ok, msg):
    if not ok:
        problems.append(msg)


kinds = {d["kind"]: d for d in docs if d}
for k in ("ServiceAccount", "Role", "RoleBinding", "DaemonSet"):
    check(k in kinds, f"missing {k}")
if problems:
    sys.exit("\n".join(problems))

ds = kinds["DaemonSet"]
tpl = ds["spec"]["template"]
pod = tpl["spec"]
containers = pod["containers"]
check(len(containers) == 1 and containers[0]["name"] == "sic", "expected exactly one container named sic")
c = containers[0]
env = {e["name"]: e.get("value") for e in c.get("env", [])}


def port(addr):
    return int(addr.rsplit(":", 1)[1])


# --- the selector, the labels and SIC's own peer discovery must be the same ---
labels = tpl["metadata"]["labels"]
check(ds["spec"]["selector"]["matchLabels"] == labels, "selector does not match the pod labels")
sel = env.get("SIC_POD_LABEL_SELECTOR", "")
check(sel == ",".join(f"{k}={v}" for k, v in labels.items()),
      f"SIC_POD_LABEL_SELECTOR {sel!r} does not match the pod labels {labels}")

# --- ports ---
listen = port(env["SIC_LISTEN"])
admin = port(env["SIC_ADMIN_LISTEN"])
check(int(env["SIC_PEER_PORT"]) == listen, "SIC_PEER_PORT differs from the SIC_LISTEN port")
check(pod.get("hostNetwork") is True, "hostNetwork must be on: containerd reaches SIC on the node's 127.0.0.1")
check(env["SIC_ADMIN_LISTEN"].startswith("127.0.0.1:"), "the plain-HTTP admin port must stay on loopback")
for p in c.get("ports", []):
    check(p["containerPort"] == listen and p.get("hostPort", listen) == listen,
          "hostPort and containerPort must both be the SIC_LISTEN port (hostNetwork)")

# --- probes: both exist and use the admin port, because the main one needs a client certificate ---
for name in ("readinessProbe", "livenessProbe"):
    pr = c.get(name)
    check(pr is not None, f"{name} is missing")
    if pr:
        h = pr.get("httpGet", {})
        check(h.get("port") == admin and h.get("scheme") == "HTTP" and h.get("path") == "/healthz",
              f"{name} must GET /healthz over HTTP on the admin port {admin}")
        check(h.get("host") == "127.0.0.1", f"{name} must target 127.0.0.1 (the admin port listens on loopback only)")
lp = c.get("livenessProbe", {})
check(lp.get("failureThreshold", 3) * lp.get("periodSeconds", 10) >= 60,
      "livenessProbe should tolerate at least a minute of failures: a restart interrupts transfers")

# --- resources ---
res = c.get("resources", {})
check("cpu" in res.get("requests", {}) and "memory" in res.get("requests", {}), "cpu and memory requests are required")
check("memory" in res.get("limits", {}), "a memory limit is required")
check("cpu" not in res.get("limits", {}), "no CPU limit: throttling makes layer hashing and pulls slower")

# --- security ---
sc = c.get("securityContext", {})
check(sc.get("readOnlyRootFilesystem") is True, "readOnlyRootFilesystem must be true")
check(sc.get("allowPrivilegeEscalation") is False, "allowPrivilegeEscalation must be false")
check(sc.get("capabilities", {}).get("drop") == ["ALL"], "capabilities must drop ALL")
check(pod.get("securityContext", {}).get("seccompProfile", {}).get("type") == "RuntimeDefault",
      "pod seccompProfile must be RuntimeDefault")
check("privileged" not in sc or sc["privileged"] is False, "must not be privileged")
if sc.get("runAsUser", 0) == 0:
    check(env.get("SIC_CONTAINERD_IMPORT") == "true",
          "root is only justified by the containerd socket; with the import off, run as non-root")

# --- the image: pinned enough, and pulled without needing the network if present ---
img = c["image"]
check("/" in img.split(":")[0] and (":" in img.rsplit("/", 1)[-1] or "@sha256:" in img), f"image {img!r} needs a registry and a tag or digest")
check(not img.endswith(":latest") and not img.endswith(":dev"), f"image {img!r} must not use :latest or :dev")
check(c.get("imagePullPolicy") == "IfNotPresent",
      "imagePullPolicy must be IfNotPresent: with SIC as the node's only mirror, 'Always' cannot pull its own image after a restart")

# --- scheduling ---
check(pod.get("priorityClassName") in ("system-node-critical", "system-cluster-critical"), "priorityClassName is missing")
check(pod.get("tolerations"), "tolerations are missing: SIC would not run on tainted nodes")

# --- every volume that is mounted exists, every one that exists is mounted ---
vols = {v["name"]: v for v in pod.get("volumes", [])}
mounts = {m["name"]: m for m in c.get("volumeMounts", [])}
check(set(vols) == set(mounts), f"volumes {sorted(vols)} and mounts {sorted(mounts)} differ")
for v in vols.values():
    if "secret" in v:
        check(v["secret"].get("defaultMode") == 0o400, f"secret volume {v['name']} should be mode 0400")
for n, m in mounts.items():
    if n in ("containerd-socket", "tls"):
        check(m.get("readOnly") is True, f"{n} must be mounted read-only")
tls = {k: env.get(k) for k in ("SIC_TLS_CERT", "SIC_TLS_KEY", "SIC_TLS_CA", "SIC_TLS_CLIENT_CERT", "SIC_TLS_CLIENT_KEY")}
check(all(v and v.startswith(mounts["tls"]["mountPath"] + "/") for v in tls.values()), "TLS paths must be under the tls mount")
check(env.get("SIC_CONTAINERD_ADDRESS") == mounts["containerd-socket"]["mountPath"], "SIC_CONTAINERD_ADDRESS must be the mounted socket")
check(env.get("SIC_CACHE_DIR", "/var/lib/steady-image-cache") == mounts["cache"]["mountPath"], "the cache volume must be mounted at SIC_CACHE_DIR")
check(env.get("SIC_INSECURE") in (None, "false"), "the sample must not turn TLS off")

# --- the pod can only do what discovery needs ---
role = kinds["Role"]
verbs = {v for r in role["rules"] for v in r["verbs"]}
check(verbs == {"get", "list", "watch"}, f"Role verbs {sorted(verbs)}: discovery only reads pods")
check(all(r["resources"] == ["pods"] for r in role["rules"]), "Role should only cover pods")
check(pod.get("serviceAccountName") == kinds["ServiceAccount"]["metadata"]["name"], "serviceAccountName differs from the ServiceAccount")

if problems:
    print(f"{path}: {len(problems)} problem(s)")
    for p in problems:
        print(" -", p)
    sys.exit(1)
print(f"{path}: ok")
