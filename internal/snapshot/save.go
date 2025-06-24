package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

const (
	defaultVolumeLifeDurationMinutes int32 = 20
)

// executeWithBeforeAfter wraps a function with configurable before and after command execution
func (s *AWSSnapshotter) executeWithBeforeAfter(ctx context.Context, description string, beforeAfterCmd []string, beforeAfterLabel string, mainFunc func() error) error {
	// Execute before command
	s.logger.Info().Msgf("%s: Checking %s before...", description, beforeAfterLabel)
	beforeOutput, err := s.runCommand(ctx, beforeAfterCmd[0], beforeAfterCmd[1:]...)
	if err != nil {
		s.logger.Warn().Msgf("Warning: failed to check %s before %s: %v. Output: %s", beforeAfterLabel, description, err, string(beforeOutput))
	} else {
		s.logger.Info().Msgf("%s: %s before:\n%s", description, beforeAfterLabel, string(beforeOutput))
	}

	// Execute main function
	mainErr := mainFunc()

	// Execute after command
	s.logger.Info().Msgf("%s: Checking %s after...", description, beforeAfterLabel)
	afterOutput, err := s.runCommand(ctx, beforeAfterCmd[0], beforeAfterCmd[1:]...)
	if err != nil {
		s.logger.Warn().Msgf("Warning: failed to check %s after %s: %v. Output: %s", beforeAfterLabel, description, err, string(afterOutput))
	} else {
		s.logger.Info().Msgf("%s: %s after:\n%s", description, beforeAfterLabel, string(afterOutput))
	}

	return mainErr
}

// executeWithDockerDiskUsage is a convenience wrapper for docker buildx disk usage reporting
func (s *AWSSnapshotter) executeWithDockerDiskUsage(ctx context.Context, description string, mainFunc func() error) error {
	return s.executeWithBeforeAfter(ctx, description, []string{"sudo", "docker", "buildx", "--builder", "runs-on", "du"}, "docker buildx disk usage", mainFunc)
}

func (s *AWSSnapshotter) CreateSnapshot(ctx context.Context, mountPoint string) (*CreateSnapshotOutput, error) {
	gitBranch := s.config.GithubRef
	s.logger.Info().Msgf("CreateSnapshot: Using git ref: %s, Instance ID: %s, MountPoint: %s", gitBranch, s.config.InstanceID, mountPoint)

	// Load volume info from JSON file
	volumeInfo, err := s.loadVolumeInfo(mountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to load volume info: %w", err)
	}

	// 2. Operations on jobVolumeID
	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		err := s.executeWithDockerDiskUsage(ctx, "CreateSnapshot", func() error {
			output, err := s.runCommand(ctx, "sudo", "docker", "buildx", "--builder", "runs-on", "prune", "--keep-storage", "12g", "-f")
			if err != nil {
				s.logger.Warn().Msgf("Warning: failed to prune docker builder: %v. Output: %s", err, string(output))
				return err
			} else {
				s.logger.Info().Msgf("CreateSnapshot: Pruned docker builder. Output: %s", string(output))
				return nil
			}
		})
		if err != nil {
			s.logger.Warn().Msgf("Warning: docker prune operation failed: %v", err)
		}

		s.logger.Info().Msgf("CreateSnapshot: Stopping docker service...")
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "stop", "docker"); err != nil {
			s.logger.Warn().Msgf("Warning: failed to stop docker (may not be running or installed): %v", err)
		}
	}

	s.logger.Info().Msgf("CreateSnapshot: Unmounting %s (from device %s, volume %s)...", mountPoint, volumeInfo.DeviceName, volumeInfo.VolumeID)
	if _, err := s.runCommand(ctx, "sudo", "umount", mountPoint); err != nil {
		dfOutput, checkErr := s.runCommand(ctx, "df", mountPoint)
		if checkErr == nil && strings.Contains(string(dfOutput), mountPoint) { // If still mounted, then error
			return nil, fmt.Errorf("failed to unmount %s: %w. Output: %s", mountPoint, err, string(dfOutput))
		}
		s.logger.Warn().Msgf("CreateSnapshot: Unmount of %s failed but it seems not mounted anymore: %v", mountPoint, err)
	} else {
		s.logger.Info().Msgf("CreateSnapshot: Successfully unmounted %s.", mountPoint)
	}

	// Update TTL tag on volume to extend until 10min from now
	_, err = s.ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{volumeInfo.VolumeID},
		Tags: []types.Tag{
			{Key: aws.String(ttlTagKey), Value: aws.String(fmt.Sprintf("%d", time.Now().Add(10*time.Minute).Unix()))},
		},
	})
	if err != nil {
		s.logger.Warn().Msgf("Failed to update TTL tag on volume %s: %v", volumeInfo.VolumeID, err)
	}

	s.logger.Info().Msgf("CreateSnapshot: Detaching volume %s...", volumeInfo.VolumeID)
	_, err = s.ec2Client.DetachVolume(ctx, &ec2.DetachVolumeInput{
		VolumeId:   aws.String(volumeInfo.VolumeID),
		InstanceId: aws.String(s.config.InstanceID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initiate detach for volume %s: %w", volumeInfo.VolumeID, err)
	}

	volumeDetachedWaiter := ec2.NewVolumeAvailableWaiter(s.ec2Client, defaultVolumeAvailableWaiterOptions) // Available state implies detached
	s.logger.Info().Msgf("CreateSnapshot: Waiting for volume %s to become available (detached)...", volumeInfo.VolumeID)
	if err := volumeDetachedWaiter.Wait(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{volumeInfo.VolumeID}}, defaultVolumeAvailableMaxWaitTime); err != nil {
		return nil, fmt.Errorf("volume %s did not become available (detach) in time: %w", volumeInfo.VolumeID, err)
	}
	s.logger.Info().Msgf("CreateSnapshot: Volume %s is detached.", volumeInfo.VolumeID)

	// 3. Create new snapshot
	currentTime := time.Now()
	s.logger.Info().Msgf("CreateSnapshot: Creating snapshot '%s' from volume %s for branch %s...", s.config.SnapshotName, volumeInfo.VolumeID, s.config.GithubRef)
	snapshotTags := append(s.defaultTags(), []types.Tag{
		{Key: aws.String(nameTagKey), Value: aws.String(s.config.SnapshotName)},
	}...)
	createSnapshotOutput, err := s.ec2Client.CreateSnapshot(ctx, &ec2.CreateSnapshotInput{
		VolumeId: aws.String(volumeInfo.VolumeID),
		TagSpecifications: []types.TagSpecification{
			{
				ResourceType: types.ResourceTypeSnapshot,
				Tags:         snapshotTags,
			},
		},
		Description: aws.String(fmt.Sprintf("Snapshot for branch %s taken at %s", s.config.GithubRef, currentTime.Format(time.RFC3339))),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot from volume %s: %w", volumeInfo.VolumeID, err)
	}
	newSnapshotID := *createSnapshotOutput.SnapshotId
	s.logger.Info().Msgf("CreateSnapshot: Snapshot %s creation initiated.", newSnapshotID)

	if volumeInfo.NewVolume {
		s.logger.Info().Msgf("CreateSnapshot: creating from a new volume, so waiting for initial snapshot completion. This may take a few minutes.")
	} else if s.config.WaitForCompletion {
		s.logger.Info().Msgf("CreateSnapshot: waiting for snapshot completion before returning.")
	} else {
		s.logger.Info().Msgf("CreateSnapshot: not waiting for snapshot completion, returning immediately.")
		return &CreateSnapshotOutput{SnapshotID: newSnapshotID}, nil
	}

	s.logger.Info().Msgf("CreateSnapshot: Waiting for snapshot %s completion...", newSnapshotID)
	snapshotCompletedWaiter := ec2.NewSnapshotCompletedWaiter(s.ec2Client, defaultSnapshotCompletedWaiterOptions)
	if err := snapshotCompletedWaiter.Wait(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: []string{newSnapshotID}}, defaultSnapshotCompletedMaxWaitTime); err != nil {
		return nil, fmt.Errorf("snapshot %s did not complete in time: %w", newSnapshotID, err)
	}
	s.logger.Info().Msgf("CreateSnapshot: Snapshot %s completed.", newSnapshotID)

	// 5. Delete the jobVolumeID (the volume that was just snapshotted)
	s.logger.Info().Msgf("CreateSnapshot: Deleting original volume %s as its state is now in snapshot %s...", volumeInfo.VolumeID, newSnapshotID)
	_, err = s.ec2Client.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: aws.String(volumeInfo.VolumeID)})
	if err != nil {
		s.logger.Warn().Msgf("Warning: Failed to delete volume %s: %v. Manual cleanup may be required.", volumeInfo.VolumeID, err)
	} else {
		s.logger.Info().Msgf("CreateSnapshot: Volume %s successfully deleted.", volumeInfo.VolumeID)
	}

	return &CreateSnapshotOutput{SnapshotID: newSnapshotID}, nil
}
