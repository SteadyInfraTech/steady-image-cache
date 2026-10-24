# Releasing

1. `CHANGELOG.md` keeps an `## Unreleased` section that every pull request with a change a user would notice adds a line to as it merges (fixes, behaviour, settings, metrics, API; not refactors or test-only changes). To release, rename that heading to the version and date (`## 1.0.1 - 2026-11-02`) and add a new empty `## Unreleased` above it. Choose the version from what is in the section: only fixes is a patch; a new setting, metric or endpoint is a minor; anything that breaks an existing deployment is a major.
2. In `deploy/daemonset.yaml`, change the image tag from `:main` to the release tag (for example `:v1.0.0`). The end-to-end test reads the image name from there, so nothing else needs to change.
3. Merge that as one pull request, with CI green (tests, the multi-architecture image build, the manifest checks and the kind end-to-end test).
4. Tag the merge commit `vMAJOR.MINOR.PATCH` and push the tag. The publish job builds `linux/amd64` and `linux/arm64`, stamps the version into the binary, and pushes `:vX.Y.Z` (and `:latest` for a version without a pre-release suffix such as `-rc.1`). A tag with a suffix is a pre-release and does not move `latest`.
5. Check the published image: `docker run --rm ghcr.io/steadyinfratech/steady-image-cache:vX.Y.Z --version` must print the version, and `docker buildx imagetools inspect` must list both platforms.
6. Create the GitHub release from the tag, with the changelog entry as its notes.
7. Leave `deploy/daemonset.yaml` on the release tag: the repository's sample always names the latest release, not the branch build.
