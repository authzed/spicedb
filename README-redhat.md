# Information about Red Hat's Fork of SpiceDB

This document captures all changes made by Red Hat that diverge from the upstream fork. It outlines why these changes were made for historical reference and to help ensure they are kept when syncing with upstream.

### OpenSSF Scorecard

GitHub's Scorecard action does not support fork repositories. The [upstream authzed/spicedb Scorecard](https://securityscorecards.dev/viewer/?uri=github.com/authzed/spicedb) is authoritative for upstream code; Red Hat-specific changes in this fork are covered by the repository's existing security scans and review controls.

&nbsp;

### Drift Tracking

The table below captures all changes to our fork from upstream. Each entry includes the affected files, what changed, why, and how to handle conflicts during upstream syncs.

**Merge actions:**
- **Keep ours**: always preserve the Red Hat version of this file
- **Re-apply**: accept upstream changes, then re-apply our specific modifications
- **Delete**: file should not exist in our fork; remove if upstream re-adds it
- **Red Hat only**: file exists only in our fork; no upstream equivalent

| File(s) | Change | Reason | Merge Action |
|---------|--------|--------|-------------|
| `.github/dependabot.yml` | Removed | Aligns with Red Hat mandates to leverage Konflux | Delete |
| `.github/renovate.json` | Replaced with our own config | Configures Mintmaker (part of Konflux) to prevent Go pkg update PRs and move to weekly updates for Dockerfile base image updates | Keep ours |
| Active workflows in `.github/workflows/` | Runner changed to `ubuntu-latest` | Authzed uses custom self-hosted runners (`depot-*`, `buildjet-*`) which we don't have access to | Re-apply |
| `.github/workflows/build-test.yaml` | Disabled: build, steelthread, analyzer-unit, WASM, protobuf, benchmark, datastoreconstest jobs (`if: false`); disabled CockroachDB/MySQL/Spanner datastore tests; changed `Dockerfile` reference to `Dockerfile.fips`; limited Postgres versions to 16/17 | Non-critical to Red Hat builds or not applicable to our deployment targets | Re-apply |
| `.github/workflows/benchmark.yaml` | Disabled (`if: false`) | Not critical to Red Hat builds | Re-apply |
| `.github/workflows/commit-messages.yaml` | Disabled (`if: false`) | Not critical to Red Hat builds | Re-apply |
| `.github/workflows/docs.yaml` | Disabled (`if: false`) | Not critical to Red Hat builds | Re-apply |
| `.github/workflows/keep-a-changelog.yaml` | Disabled (`if: false`) | Not critical to Red Hat builds | Re-apply |
| `.github/workflows/lint.yaml` | Disabled (`if: false`) | Not critical to Red Hat builds | Re-apply |
| `.github/workflows/release-windows.yaml` | Disabled (`if: false`) | Not critical to Red Hat builds | Re-apply |
| `.github/workflows/cla.yaml` | Removed | Not applicable to our fork | Delete |
| `.github/workflows/labeler.yaml` | Removed | Not applicable to our fork | Delete |
| `.github/workflows/nightly.yaml` | Removed | Not applicable to our fork | Delete |
| `.github/workflows/release.yaml` | Removed | Not applicable to our fork | Delete |
| `.github/workflows/security.yaml` | Removed | Replaced by our own security scanning workflow | Delete |
| `.github/workflows/wasm.yaml` | Removed | Not applicable to our fork | Delete |
| `.github/workflows/security-scanning.yml` | Added | Required ConsoleDot platform security workflow for CVE scanning | Red Hat only |
| `SECURITY.md` | Prepended Red Hat Product Security reporting and supported-version guidance while preserving the upstream policy and threat model | Routes fork and Red Hat build reports through the required disclosure process | Re-apply |
| `.github/workflows/tag-release.yaml` | Added | Automatically creates a git tag on the merge commit whenever `SYNC.md` changes on `main`; required for accurate CVE scanning (Go VCS stamping reads the nearest ancestor tag to set `debug.BuildInfo.Main.Version`) | Red Hat only |
| `.tekton/spicedb-pull-request.yaml`, `.tekton/spicedb-push.yaml` | Added; `fetchTags: "true"` and `depth: "0"` added to `clone-repository` task params | Konflux PR and merge build pipelines; `fetchTags` + `depth: "0"` (full clone) ensure Go's VCS stamping can walk history back to the nearest semver tag and embed it as `debug.BuildInfo.Main.Version` — without this CVE scanners see a `v0.0.0` pseudo-version and generate false positives. A shallow clone with only `fetchTags` is not sufficient because Go must walk the ancestor chain to find the tag. Konflux automated updates may remove these params — re-add them if missing. | Red Hat only |
| `Dockerfile.fips` | Added | FIPS-compliant builds using Hummingbird base images for Konflux; grpc-health-probe built from source (pinned to a `grpc-ecosystem/grpc-health-probe` tag matching upstream `Dockerfile`, updated during syncs) to enable FIPS compilation, reads version from `SYNC.md` and embeds it via `-ldflags` so `spicedb version` reports the correct release tag | Red Hat only |
| `magefiles/test.go` | Increased timeouts (unit: 20m, integration: 30m, consistency: 20m) | Tests fail with short timeouts on smaller runners | Re-apply |
| `scripts/redhat-diff.sh` | Added | Script to isolate Red Hat-specific changes from upstream sync PRs for easier code review | Red Hat only |
| `CLAUDE.md` | Replaced with our own | Contains Red Hat-specific merge conflict resolution rules for upstream syncs | Keep ours |
| `.claude/skills/sync-upstream/SKILL.md` | Added | Claude skill to handle the upstream syncing process | Red Hat only |
| `README-redhat.md` | Added | Documents all Red Hat fork changes and rationale | Red Hat only |
| `SYNC.md` | Added | Tracks the current upstream version synced to this fork | Red Hat only |
| `.yamllint` | Added | YAML linting configuration | Red Hat only |

&nbsp;

### More on our Dockerfile and Builds

**Hummingbird Base Images**

Our Dockerfiles use Red Hat Hummingbird images for both building and running the container. The builder stage uses `registry.access.redhat.com/hi/go:1.26.6-fips`, which provides a Go toolchain that automatically embeds a validated FIPS module into all compiled binaries. The runtime stage uses `registry.access.redhat.com/hi/core-runtime:2.42-openssl-fips`, a minimal image with OpenSSL FIPS support. At runtime, `GODEBUG=fips140=on` is set to activate FIPS mode.

This replaces the previous approach of UBI 9 base images with Go Toolset, `CGO_ENABLED=1`, `GOEXPERIMENT=strictfipsruntime,boringcrypto`, and the `fips_enabled` build tag. With Hummingbird, FIPS compliance is handled transparently by the Go toolchain and runtime image, eliminating the need for manual FIPS build flags.

**Go and Base Image Updates**

The Go version in `go.mod` should be compatible with the version of Go available in the Hummingbird builder image (`hi/go`). Base images should be updated as new validated versions become available.

For more info on FIPS compliance at Red Hat:
* [Red Hat FIPS Compliance](https://access.redhat.com/compliance/fips)

&nbsp;

### Keeping SYNC.md Up to Date

The [SYNC.md](SYNC.md) file tracks the current upstream [authzed/spicedb](https://github.com/authzed/spicedb/) version that has been merged into this fork. This file must be updated whenever a new upstream sync is performed.

**When to Update**

Update `SYNC.md` as part of any PR that merges changes from the upstream [authzed/spicedb](https://github.com/authzed/spicedb/) repository.

**How to Update**

1. Set `TAG` to the upstream release tag being synced (eg. `v1.47.1`)
2. Set `COMMIT_SHA` to the full commit SHA of the upstream commit being merged

### Updating this File

If changes are made to our fork that diverge from upstream that are not captured in this README, make sure to update this file with any relevant changes. Be sure to capture the change and reason in the table above.

An easy way to capture differences is to use `scripts/redhat-diff.sh` which compares the merge branch against the upstream tag and shows only Red Hat-specific changes:

```bash
# Show summary of Red Hat changes for this sync
./scripts/redhat-diff.sh --stat

# Show all cumulative Red Hat changes vs upstream
./scripts/redhat-diff.sh --all --stat
```
