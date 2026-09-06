# Matrix build-state snapshots

Use one snapshot stream per independent matrix workload, with a stable absolute mount path. Mount an isolated root such as `/mnt/build-state`; keep the source worktree in `/mnt/build-state/workspace`. Mounting over `GITHUB_WORKSPACE` can interfere with checkout, local actions, and post-step unmounting. Use one snapshot invocation per job; this change does not add multi-volume device discovery.

For new RunsOn v3.2+ deployments, first evaluate [managed sticky disks](https://runs-on.com/docs/runners/capabilities/sticky-disks/). Their `rust` mode persists Cargo registry/Git inputs; target or tool directories require separate paths. Managed sticky disks do not expose this action's Git-path save policy. This legacy action requires runner-side EC2/EBS permissions and is incompatible with the optional sticky-disk isolation setting that removes them. See the [v3.2 migration guide](https://runs-on.com/docs/maintenance/v3-2-upgrade/).

## Identity and fallback

`key` names a workload stream and `version` remains the manual invalidation setting. With a key, lookup identity incorporates the key, normalized mount path, and version, in addition to the existing repository, ref, stack, architecture, and platform tags. The combined identity is hashed into the existing `runs-on-snapshot-version` tag so cleanup can continue using that dimension. Omitting `key` preserves the legacy version-only behavior, including its lack of path scoping. Adopting a key starts a cold stream; it does not silently reuse an unkeyed snapshot.

```yaml
with:
  path: /mnt/build-state
  key: cargo-${{ matrix.profile }}
  version: rust-build-v1
  restore-keys: |
    cargo-compatible-fallback
  default-branch-fallback: true
```

Fallback keys are complete keys, not prefix matches. Choose only keys with compatible build layouts. Lookup tries the primary key and fallback keys on the current branch, then the same order on the default branch, then a blank volume. Set `default-branch-fallback: false` to disable the branch fallback. Candidates retain repository, stack, platform, path, and version scope; snapshots smaller than the requested volume are skipped. This does not add filesystem expansion.

The main step exposes `restored`, `restored-from`, `restored-branch`, `restored-snapshot-id`, and `volume-id`. `restored-from` is `branch`, `restore-key`, `default-branch`, `default-branch-restore-key`, or `empty`. These report filesystem restoration, not a guarantee that Cargo will consider a build fresh.

## Decide whether to save after the build

```yaml
with:
  path: /mnt/build-state
  key: cargo-${{ matrix.profile }}
  save: auto
  save-if: git-paths-changed
  git-repository: /mnt/build-state/workspace
  git-head: ${{ github.sha }}
  git-paths: |
    Cargo.toml
    Cargo.lock
    rust-toolchain.toml
    .cargo/**
    src/**
    crates/**
```

The post step compares committed files against the source recorded in the restored snapshot. Include every workspace dependency, build script, configuration file, and other tracked input that can affect the matrix entry. Git pathspecs apply at the repository root. This is a save optimization; it never skips the build itself. Untracked/generated inputs, environment changes, features, toolchain updates, and build-command semantics need an explicit `version` bump or `force-save: true` when they should refresh a snapshot.

With `save: auto` and `save-if: git-paths-changed`, matching commits or unchanged relevant paths skip snapshot creation. Missing/malformed metadata, a missing base in shallow history, Git comparison errors, a different key or path list, and a checkout that does not match `git-head` conservatively save. A checkout mismatch is not recorded as an authoritative source SHA. Metadata lives at `<path>/.runs-on-snapshot/source.json`; it contains a source commit and policy fingerprint, not credentials.

`force-save: true` bypasses a smart-save skip. `save: false` still wins, so a restore-only trust policy cannot be overridden by `force-save`. `save: true` always saves and `save-if: always` is the default automatic policy. Static expressions can still restrict writers to trusted branches; the example does that before applying the runtime policy.

Skipped saves still unmount, detach, and delete the job volume. Restore, save, and cleanup errors fail the action instead of only annotating the log. The existing `post-if: success()` remains: failed or cancelled jobs do not publish snapshots through this action, and their residual volume cleanup still depends on the RunsOn lifecycle. Post-step save decisions are logged with a reason; they cannot be consumed by steps that already finished. This does not add a separate save action, user retention controls, or a guarantee that concurrent writes to one stream merge. Partition matrix writers and serialize concurrent jobs targeting the same stream when ordering matters.

## Keep ordinary tool setup steps

The [Cargo workflow example](../examples/cargo-build-state.yml) gives the standard Rust installer persistent locations through `CARGO_HOME` and `RUSTUP_HOME` before setup runs. On a cold volume the installer populates those directories; on a warm volume it verifies/reuses the installation and restores its environment. It continues using the normal setup action and adds its binary path for later steps. Keep the Rust channel or exact version in the workflow and rotate the stream when changing build semantics.

For other setup actions, use the installer's documented data/cache root and configure it before restore/setup. GitHub toolkit-based installers commonly use `RUNNER_TOOL_CACHE` (and some consumers also use `AGENT_TOOLSDIRECTORY`); mise uses `MISE_DATA_DIR`. Each tool has its own environment activation and layout contract. Do not assume restoring a directory exports PATH or proves the tool is installed. Keep helper tools, package download caches, Cargo target state, and credentials as distinct paths; avoid snapshotting the entire home directory.

Use `runs-on/action`'s managed `tool-cache` mode only after checking a release that actually contains it. [Action PR #54](https://github.com/runs-on/action/pull/54) and [PR #55](https://github.com/runs-on/action/pull/55) were still open at the September 6, 2026 review; their proposed behavior is separate from this legacy action's path layout.

The example first checks out into the ordinary GitHub workspace with credential persistence disabled, then fetches that local repository into the restored worktree. It avoids storing an authenticated remote or HTTP header in the snapshot. A matching, clean worktree is left alone; otherwise checkout updates tracked contents while preserving unrelated build outputs. Cargo can still rebuild after source, toolchain, path, feature, or build-script changes. Avoid adding blanket `git clean` or overlapping `rust-cache` ownership of this tree.

Before saving any tool/Cargo home, remove registry credentials, generated credential-bearing configuration, and other secrets. Prefer a separate ephemeral credential location where the tool supports it. The example uses public Cargo dependencies and scrubs the standard Cargo credential files before a successful post step; private registries require adapting that boundary explicitly.

## Validation boundary

Unit tests cover keyed identity, fallback order, paginated snapshot selection, and real-Git save decisions. EBS attach/mount/detach/delete, service-side cleanup grouping, cancellation, cross-branch restore, and cold/warm installer behavior still need a RunsOn integration run before release. Keep that check separate from comparing Rust build performance.
