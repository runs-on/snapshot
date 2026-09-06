package main

import (
	"context"
	"flag"
	"os"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/runs-on/snapshot/internal/config"
	"github.com/runs-on/snapshot/internal/snapshot"
	"github.com/sethvargo/go-githubactions"
)

// handleMainExecution contains the original main logic.
func handleMainExecution(action *githubactions.Action, ctx context.Context, logger *zerolog.Logger) {
	cfg := config.NewConfigFromInputs(action)

	if cfg.Path != "" {
		action.Infof("Restoring volume for %s...", cfg.Path)
		snapshotter, err := snapshot.NewAWSSnapshotter(ctx, logger, cfg)
		if err != nil {
			action.Fatalf("Failed to create snapshotter: %v", err)
		} else {
			action.Infof("Creating snapshot for %s", cfg.Path)
			snapshotOutput, err := snapshotter.RestoreSnapshot(ctx, cfg.Path)
			if err != nil {
				action.Fatalf("Failed to restore snapshot for %s: %v", cfg.Path, err)
			} else {
				action.Infof("Snapshot restored into volume %s", snapshotOutput.VolumeID)
				action.SetOutput("restored", strconv.FormatBool(!snapshotOutput.NewVolume))
				action.SetOutput("restored-from", snapshotOutput.Source)
				action.SetOutput("restored-branch", snapshotOutput.Branch)
				action.SetOutput("restored-snapshot-id", snapshotOutput.SnapshotID)
				action.SetOutput("volume-id", snapshotOutput.VolumeID)
			}
		}
	}

	action.Infof("Action finished.")
}

// handlePostExecution contains the logic for the post-execution phase.
func handlePostExecution(action *githubactions.Action, ctx context.Context, logger *zerolog.Logger) {
	action.Infof("Running post-execution phase...")
	cfg := config.NewConfigFromInputs(action)

	if cfg.Path != "" {
		action.Infof("Snapshotting volume for %s...", cfg.Path)
		snapshotter, err := snapshot.NewAWSSnapshotter(ctx, logger, cfg)
		if err != nil {
			action.Fatalf("Failed to create snapshotter: %v", err)
		} else {
			snapshot, err := snapshotter.CreateSnapshot(ctx, cfg.Path)
			if err != nil {
				action.Fatalf("Failed to snapshot volumes: %v", err)
			} else if snapshot.SnapshotID == "" {
				action.Infof("Snapshot save skipped: %s; volume cleaned up.", snapshot.Reason)
			} else {
				action.Infof("Snapshot created: %s. Note that it might take a few minutes to be available for use.", snapshot.SnapshotID)
			}
		}
	}
	action.Infof("Post-execution phase finished.")
}

func main() {
	ctx := context.Background()
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	postFlag := flag.Bool("post", false, "Indicates the post-execution phase")
	flag.Parse()

	action := githubactions.New()

	if *postFlag {
		handlePostExecution(action, ctx, &logger)
	} else {
		handleMainExecution(action, ctx, &logger)
	}
}
