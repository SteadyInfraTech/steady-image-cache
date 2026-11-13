# Contributing

This is how a change gets made here. The pull request is the unit of work: it carries the code, the tests that prove it, the documentation and the changelog line, and merges when CI is green and the review findings are answered.

## Layout

| Path | What is in it |
|---|---|
| `cmd/sic`, `internal/*` | the program: proxy, store, peers (discovery, gossip), containerd import, `init-node`, metrics, TLS |
| `deploy/` | the sample DaemonSet, containerd `hosts.toml` samples, Prometheus rules and the Grafana dashboard |
| `charts/steady-image-cache` | the Helm chart |
| `test/` | end-to-end scripts (`e2e`), chart tests (`helm`), alert tests (`monitoring`) |
| `docs/` | redirects only; the project website, https://steadyinfra.com/sic/, is outside this repository |
| `README.md` | the reference: behaviour, settings, deployment. `CHANGELOG.md`: what changed per release |

## Before opening a pull request

```bash
gofmt -l .                       # no output
go vet ./...
go test -race ./...
golangci-lint run ./...          # version pinned in .github/workflows/ci.yml; config in .golangci.yml
```

Go: `go.mod` names the language version CI uses (1.26.10); the Dockerfile builds the release with the Go 1.27 image. The module proxy is all CI needs; resolving `go.mod` changes where it is not reachable is done by a CI job that prints the result.

The end-to-end scripts need `docker`, `kind`, `kubectl`, `openssl` (and `helm` for the chart) and take 5–15 minutes; CI runs them, so run one locally only when you are working on it.

## What a change needs

* **A test that fails without the change.** Write the test, revert the fix, and see it fail: a test that has never failed has not been shown to test anything. Several tests here were checked this way, and the end-to-end legs have mutants (a deliberately broken variant that must turn the run red) in the history of their pull requests.
* **A line in `CHANGELOG.md` under `## Unreleased`** for anything an operator would notice: fixes, behaviour, settings, metrics, API paths, dependencies that change what runs. Not for refactors or test-only changes. If an operator has to *do* something, say so first in the entry.
* **The documentation that describes it.** A new setting goes in the README's settings table and its section, and in the chart's `values.yaml` and `templates/` if the chart exposes it. A new metric goes in the README's metrics list, and in `deploy/monitoring/` if it should be graphed or alerted on. A changed default is a changelog entry.
* **Samples are tested, so edit them with that in mind.** `deploy/daemonset.yaml` and `deploy/containerd/*.toml` are what the end-to-end tests install; the version containerd 1.7 refused (a `hosts.toml` with no `[host]` table) was found because the test uses the file as shipped.
* **The public interface is the settings (`SIC_*`), the metric names and the HTTP paths** and follows semantic versioning; a change that breaks an existing deployment is a major version.

## CI

Every pull request runs the checks that apply to the files it changes. What each is for, and which to read when it fails:

| Workflow | Checks | When |
|---|---|---|
| `ci.yml` | `lint`, `test` (race detector, a 20-second run of each fuzz target), `vulncheck` (govulncheck; nothing is accepted: an exception needs an id and a reason in the workflow) | always |
| `release.yml` | the multi-architecture image builds; on a tag or `main` it is published with an SBOM and provenance, signed with cosign, and the job verifies both | always, publishes on `v*` tags |
| `deploy.yml` | the sample manifests are valid | `deploy/**` |
| `e2e.yml` | the three-node kind test, phases 1–4 | code and deployment files |
| `e2e-containerd.yml` | the same on containerd 1.7 and 2.x nodes | code and deployment files, weekly |
| `e2e-operations.yml` | roll back to the previous release and forward, mixed versions | code and manifest |
| `e2e-helm.yml` | the chart on kind with cert-manager: install, init-node, certificate renewal | chart and code |
| `helm.yml` | chart lint, templating, schema checks, `promtool` on the alert rules; publishes and signs the chart on a tag | chart, monitoring files |
| `e2e-registries.yml` | pulls from real public registries (needs the internet, not a gate for pull requests) | tags, weekly, by hand |
| `store-benchmark.yml` | the store read benchmark | `internal/store/**` |

The *When* column is a summary. The authority is the `on:` block at the top of each workflow file: the end-to-end workflows also run when `cmd/**`, `internal/**`, `Dockerfile`, `go.mod`, `go.sum`, `test/e2e/**` (including `kind-config.yaml`) or the workflow file itself change, and `deploy.yml` and `helm.yml` have their own path lists. A change to a test script runs the jobs that use it.

When a check is red:

* **Read the failure; do not just rerun.** A rerun hides a flake that will come back, or a real finding. A timing test needs a margin of ten times or more over what a loaded runner can do (`TestSlowButSteadyTransferSucceeds` failed at four). A fuzz run that ends with `context deadline exceeded` and no saved input is retried once by the workflow; a saved input is a finding and fails at once.
* **The end-to-end scripts print the stage and the pod, containerd and registry logs as annotations** on the run, which is readable through the API when the job log is not at hand.
* **The end-to-end tests turn the disk reserve off** (`SIC_MIN_FREE_*`), because a runner with a nearly full disk made SIC refuse to cache a 600-byte blob. Disk pressure is covered by unit tests.
* A pull request that does not follow its branch after a GitHub outage is fixed by closing and reopening it.

## Review

Every pull request is reviewed by CodeRabbit. Its findings arrive in four places, and all four are read: the inline threads, the review body (nitpicks, and comments on lines outside the diff), the summary comment, and its "follow-up" comments after a later push. Each finding is checked against the code before anything is changed; a valid one is fixed with a test, an invalid one is answered with the reason. Replies say what was done, so that the thread can be closed.

## Dependencies

Dependabot opens one grouped pull request a week each for GitHub Actions (pinned by commit), Go modules and the Dockerfile's base image. Actions are pinned by SHA with a version comment (cosign's installer included; the cosign version it installs is pinned in the workflows too). Signing is keyless and needs `id-token: write` on the publish jobs only. The containerd client is `github.com/containerd/containerd/v2`; the end-to-end test runs it against containerd 1.7 and 2.x daemons, so a bump of either is checked by CI.

## Releasing

See `RELEASING.md`. The maintainers tag and publish releases.
