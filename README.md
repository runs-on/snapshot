# runs-on/snapshot

GitHub Action to snapshot and restore entire folders on self-hosted runners.

To be used with [RunsOn](https://runs-on.com). Requires version v2.8.3+.

## Usage

```yaml
name: Docker build with snapshots
jobs:
  long-docker-build:
    runs-on: runs-on=${{ github.run_id }}/runner=2cpu-linux-x64
    steps:
      - uses: actions/checkout@v4
      # snapshot action
      - uses: runs-on/snapshot@v1
        with:
          path: /var/lib/docker
      - uses: docker/setup-buildx-action@v3
        with:
          name: runs-on
          keep-state: true
      - uses: docker/build-push-action@v4
        with:
          context: .
```

## Inputs

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| path | Path to the directory to snapshot. Must be an absolute path. | Yes | - |
| version | Version of the snapshot to use. Can be bumped to force a new initial snapshot | No | v1 |
| volume_type | Type of volume to use for the snapshot | No | gp3 |
| volume_iops | IOPS to use for the volume | No | 3000 |
| volume_throughput | Throughput to use for the volume | No | 750 |
| volume_size | Size (in GiB) of the volume to use for the snapshot | No | 40 |
| volume_initialization_rate | Initialization rate to use for the volume. Useful for very large volumes. 100 MB/s - 200 MB/s: $0.00240/GB, 201 MB/s - 300 MB/s $0.00360/GB | No | 0 |
| wait_for_completion | Wait for snapshot completion before exiting. Note that the first snapshot will always be waited for | No | false |

## Snapshot selection

When restoring a snapshot, the most recent snapshot for the current branch is fetched. If none is found, the most recent snapshot for the repository default branch will be taken. If none found, a new empty volume is used instead.

## Snapshot cleanup

Volume and snapshot cleanup is performed by the RunsOn service that lives in your AWS account.

Only the latest snapshot from a branch is kept, and volumes are deleted 20 minutes after the snapshot has been taken.

Snapshots older than 10 days are always removed (in case the branch no longer see any activity).

## Additional notes

* On the first run, there will be an additional delay because the action will forcibly wait for the completion of the first snapshot, which takes the most time (further snapshots are incremental). This is technically not required, but will be less confusing if a second job comes up right after and you start from an empty volume again, because the first snapshot is still being created.
* Snapshot and restore speed is highly dependent on the volume type, iops, throughput, and used size. Feel free to experiment with those. Default values are a balance between good speed, and very low price.