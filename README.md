# runs-on/snapshot

GitHub Action to snapshot and restore entire folders on self-hosted runners.

To be used with [RunsOn](https://runs-on.com).

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
      # setup-buildx-action from a PR that will be merged soon?
      - uses: aptos-labs/setup-buildx-action@balaji/retain-cache
        with:
          name: runs-on
          keep-state: true
      - uses: docker/build-push-action@v4
        with:
          context: .
```

Look at the action.yml file for more details on the inputs and outputs.