package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/runs-on/snapshot/internal/utils"
)

// RestoreSnapshot finds the latest snapshot for the current git branch,
// creates a volume from it (or a new volume if no snapshot exists),
// attaches it to the instance, and mounts it to the specified mountPoint.
func (s *AWSSnapshotter) RestoreSnapshot(ctx context.Context, mountPoint string) (*RestoreSnapshotOutput, error) {
	gitBranch := s.config.GithubRef
	s.logger.Info().Msgf("RestoreSnapshot: Using git ref: %s", gitBranch)

	var err error

	var newVolume *types.Volume
	var volumeIsNewAndUnformatted bool
	// 1. Find latest snapshot for branch
	filters := []types.Filter{
		{Name: aws.String("status"), Values: []string{string(types.SnapshotStateCompleted)}},
	}
	for _, tag := range s.defaultTags() {
		filters = append(filters, types.Filter{Name: aws.String(fmt.Sprintf("tag:%s", *tag.Key)), Values: []string{*tag.Value}})
	}
	s.logger.Info().Msgf("RestoreSnapshot: Searching for the latest snapshot for branch: %s and filters: %s", gitBranch, utils.PrettyPrint(filters))
	snapshotsOutput, err := s.ec2Client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
		Filters:  filters,
		OwnerIds: []string{"self"}, // Or specific account ID if needed
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe snapshots for branch %s: %w", gitBranch, err)
	}

	var latestSnapshot *types.Snapshot
	if len(snapshotsOutput.Snapshots) > 0 {
		// Find most recent snapshot by comparing timestamps
		latestSnapshot = &snapshotsOutput.Snapshots[0]
		for _, snap := range snapshotsOutput.Snapshots {
			if snapTime := snap.StartTime; snapTime.After(*latestSnapshot.StartTime) {
				latestSnapshot = &snap
			}
		}
		s.logger.Info().Msgf("RestoreSnapshot: Found latest snapshot %s for branch %s", *latestSnapshot.SnapshotId, gitBranch)
	} else if s.config.RunnerConfig.DefaultBranch != "" {
		// Try finding snapshot from default branch
		if err := replaceFilterValues(filters, "tag:"+snapshotTagKeyBranch, []string{s.getSnapshotTagValueDefaultBranch()}); err != nil {
			return nil, fmt.Errorf("failed to find default branch filter: %w", err)
		}

		s.logger.Info().Msgf("RestoreSnapshot: No snapshot found for branch %s, trying default branch %s with filters: %s", gitBranch, s.config.RunnerConfig.DefaultBranch, utils.PrettyPrint(filters))

		defaultBranchSnapshotsOutput, err := s.ec2Client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
			Filters:  filters,
			OwnerIds: []string{"self"},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to describe snapshots for default branch %s: %w", s.config.RunnerConfig.DefaultBranch, err)
		}

		if len(defaultBranchSnapshotsOutput.Snapshots) > 0 {
			latestSnapshot = &defaultBranchSnapshotsOutput.Snapshots[0]
			for _, snap := range defaultBranchSnapshotsOutput.Snapshots {
				if snapTime := snap.StartTime; snapTime.After(*latestSnapshot.StartTime) {
					latestSnapshot = &snap
				}
			}
			s.logger.Info().Msgf("RestoreSnapshot: Found latest snapshot %s from default branch %s", *latestSnapshot.SnapshotId, s.config.RunnerConfig.DefaultBranch)
		} else {
			s.logger.Info().Msgf("RestoreSnapshot: No existing snapshot found for branch %s or default branch %s. A new volume will be created.", gitBranch, s.config.RunnerConfig.DefaultBranch)
		}
	}

	commonVolumeTags := append(s.defaultTags(), []types.Tag{
		{Key: aws.String(nameTagKey), Value: aws.String(s.config.VolumeName)},
		{Key: aws.String(ttlTagKey), Value: aws.String(fmt.Sprintf("%d", time.Now().Add(time.Duration(defaultVolumeLifeDurationMinutes)*time.Minute).Unix()))},
	}...)

	s.logger.Info().Msgf("RestoreSnapshot: common volume tags: %s", utils.PrettyPrint(commonVolumeTags))

	// Use snapshot only if its size is at least the default volume size, otherwise create a new volume
	// TODO: maybe just expand the volume size to snapshot size + 10GB, and resize disk
	if latestSnapshot != nil && latestSnapshot.VolumeSize != nil && *latestSnapshot.VolumeSize >= s.config.VolumeSize {
		// 2. Create Volume from Snapshot
		s.logger.Info().Msgf("RestoreSnapshot: Creating volume from snapshot %s", *latestSnapshot.SnapshotId)
		createVolumeInput := &ec2.CreateVolumeInput{
			SnapshotId:       latestSnapshot.SnapshotId,
			AvailabilityZone: aws.String(s.config.Az),
			VolumeType:       s.config.VolumeType,
			Iops:             aws.Int32(s.config.VolumeIops),
			TagSpecifications: []types.TagSpecification{
				{ResourceType: types.ResourceTypeVolume, Tags: commonVolumeTags},
			},
		}
		// Throughput is only supported for gp3 volumes
		if s.config.VolumeType == types.VolumeTypeGp3 {
			createVolumeInput.Throughput = aws.Int32(s.config.VolumeThroughput)
		}
		if s.config.VolumeInitializationRate > 0 {
			createVolumeInput.VolumeInitializationRate = aws.Int32(s.config.VolumeInitializationRate)
		}
		createVolumeOutput, err := s.ec2Client.CreateVolume(ctx, createVolumeInput)
		if err != nil {
			return nil, fmt.Errorf("failed to create volume from snapshot %s: %w", *latestSnapshot.SnapshotId, err)
		}
		newVolume = &types.Volume{VolumeId: createVolumeOutput.VolumeId}
		volumeIsNewAndUnformatted = false // Volume from snapshot is already formatted
		s.logger.Info().Msgf("RestoreSnapshot: Created volume %s from snapshot %s", *newVolume.VolumeId, *latestSnapshot.SnapshotId)
	} else {
		// 3. No snapshot found, create a new volume
		s.logger.Info().Msgf("RestoreSnapshot: Creating a new blank volume")
		createVolumeInput := &ec2.CreateVolumeInput{
			AvailabilityZone: aws.String(s.config.Az),
			VolumeType:       s.config.VolumeType,
			Size:             aws.Int32(s.config.VolumeSize),
			Iops:             aws.Int32(s.config.VolumeIops),
			TagSpecifications: []types.TagSpecification{
				{ResourceType: types.ResourceTypeVolume, Tags: commonVolumeTags},
			},
		}
		// Throughput is only supported for gp3 volumes
		if s.config.VolumeType == types.VolumeTypeGp3 {
			createVolumeInput.Throughput = aws.Int32(s.config.VolumeThroughput)
		}
		createVolumeOutput, err := s.ec2Client.CreateVolume(ctx, createVolumeInput)
		if err != nil {
			return nil, fmt.Errorf("failed to create new volume: %w", err)
		}
		newVolume = &types.Volume{VolumeId: createVolumeOutput.VolumeId}
		volumeIsNewAndUnformatted = true // New volume needs formatting
		s.logger.Info().Msgf("RestoreSnapshot: Created new blank volume %s", *newVolume.VolumeId)
	}

	defer func() {
		s.logger.Info().Msgf("RestoreSnapshot: Deferring cleanup of volume %s", *newVolume.VolumeId)
		if err != nil {
			s.logger.Error().Msgf("RestoreSnapshot: Error: %v", err)
			if newVolume != nil {
				s.logger.Info().Msgf("RestoreSnapshot: Deleting volume %s", *newVolume.VolumeId)
				_, err := s.ec2Client.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: newVolume.VolumeId})
				if err != nil {
					s.logger.Error().Msgf("RestoreSnapshot: Error deleting volume %s: %v", *newVolume.VolumeId, err)
				}
			}
		}
	}()

	// 4. Wait for volume to be 'available'
	s.logger.Info().Msgf("RestoreSnapshot: Waiting for volume %s to become available...", *newVolume.VolumeId)
	volumeAvailableWaiter := ec2.NewVolumeAvailableWaiter(s.ec2Client, defaultVolumeAvailableWaiterOptions)
	if err := volumeAvailableWaiter.Wait(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{*newVolume.VolumeId}}, defaultVolumeAvailableMaxWaitTime); err != nil {
		return nil, fmt.Errorf("volume %s did not become available in time: %w", *newVolume.VolumeId, err)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s is available.", *newVolume.VolumeId)

	// 5. Attach Volume
	s.logger.Info().Msgf("RestoreSnapshot: Attaching volume %s to instance %s as %s", *newVolume.VolumeId, s.config.InstanceID, suggestedDeviceName)
	attachOutput, err := s.ec2Client.AttachVolume(ctx, &ec2.AttachVolumeInput{
		Device:     aws.String(suggestedDeviceName),
		InstanceId: aws.String(s.config.InstanceID),
		VolumeId:   newVolume.VolumeId,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to attach volume %s to instance %s: %w", *newVolume.VolumeId, s.config.InstanceID, err)
	}
	actualDeviceName := *attachOutput.Device
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s attach initiated, device hint: %s. Waiting for attachment...", *newVolume.VolumeId, actualDeviceName)

	volumeInUseWaiter := ec2.NewVolumeInUseWaiter(s.ec2Client, defaultVolumeInUseWaiterOptions)
	err = volumeInUseWaiter.Wait(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []string{*newVolume.VolumeId},
		Filters: []types.Filter{
			{
				Name:   aws.String("attachment.status"),
				Values: []string{"attached"},
			},
		},
	}, defaultVolumeInUseMaxWaitTime)
	if err != nil {
		return nil, fmt.Errorf("volume %s did not attach successfully and current state unknown: %w", *newVolume.VolumeId, err)
	}
	// Fetch volume details again to confirm device name, as the attachOutput.Device might be a suggestion
	// and the waiter confirms attachment, not necessarily the final device name if it changed.
	descVolOutput, descErr := s.ec2Client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{*newVolume.VolumeId}})
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s attachments: %v", *newVolume.VolumeId, descVolOutput.Volumes[0].Attachments)
	if descErr == nil && len(descVolOutput.Volumes) > 0 && len(descVolOutput.Volumes[0].Attachments) > 0 {
		actualDeviceName = *descVolOutput.Volumes[0].Attachments[0].Device
	} else {
		return nil, fmt.Errorf("volume %s did not attach successfully and current state unknown: %w", *newVolume.VolumeId, err)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s attached as %s.", *newVolume.VolumeId, actualDeviceName)

	// Windows and Linux handle mounting differently
	if s.platform() == "windows" {
		return s.restoreSnapshotWindows(ctx, newVolume, actualDeviceName, mountPoint, volumeIsNewAndUnformatted)
	}

	// Linux mounting logic
	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		// 6. Mounting & Docker
		s.logger.Info().Msgf("RestoreSnapshot: Stopping docker service...")
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "stop", "docker"); err != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: failed to stop docker (may not be running or installed): %v", err)

		}
	}

	s.logger.Info().Msgf("RestoreSnapshot: Attempting to unmount %s (defensive)", mountPoint)
	if _, err := s.runCommand(ctx, "sudo", "umount", mountPoint); err != nil {
		s.logger.Warn().Msgf("RestoreSnapshot: Defensive unmount of %s failed (likely not mounted): %v", mountPoint, err)
	}

	// display disk configuration
	s.logger.Info().Msgf("RestoreSnapshot: Displaying disk configuration...")

	// actual device name is the last entry from `lsblk -d -n -o PATH,MODEL` that has a MODEL = 'Amazon Elastic Block Store'
	lsblkOutput, err := s.runCommand(ctx, "lsblk", "-d", "-n", "-o", "PATH,MODEL")
	if err != nil {
		s.logger.Warn().Msgf("RestoreSnapshot: Failed to display disk configuration: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(lsblkOutput)), "\n") {
		s.logger.Info().Msgf("RestoreSnapshot: lsblk output: %s", line)
		fields := strings.SplitN(line, " ", 2)
		s.logger.Info().Msgf("RestoreSnapshot: fields: %v", fields)
		// first volume is the root volume, so we need to skip it
		if len(fields) > 1 && fields[1] == "Amazon Elastic Block Store" {
			s.logger.Info().Msgf("RestoreSnapshot: Found volume: %s", fields[0])
			actualDeviceName = fields[0]
		}
	}
	s.logger.Info().Msgf("RestoreSnapshot: Actual device name: %s", actualDeviceName)

	// Save volume info to JSON file
	volumeInfo := &VolumeInfo{
		VolumeID:   *newVolume.VolumeId,
		DeviceName: actualDeviceName,
		MountPoint: mountPoint,
		NewVolume:  volumeIsNewAndUnformatted,
	}
	if err := s.saveVolumeInfo(volumeInfo); err != nil {
		s.logger.Warn().Msgf("RestoreSnapshot: Failed to save volume info: %v", err)
	}

	if volumeIsNewAndUnformatted {
		s.logger.Info().Msgf("RestoreSnapshot: Formatting new volume %s (%s) with ext4...", *newVolume.VolumeId, actualDeviceName)
		if _, err := s.runCommand(ctx, "sudo", "mkfs.ext4", "-F", actualDeviceName); err != nil { // -F to force if already formatted by mistake or small
			return nil, fmt.Errorf("failed to format device %s: %w", actualDeviceName, err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Device %s formatted.", actualDeviceName)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Creating mount point %s if it doesn't exist...", mountPoint)
	if _, err := s.runCommand(ctx, "sudo", "mkdir", "-p", mountPoint); err != nil {
		return nil, fmt.Errorf("failed to create mount point %s: %w", mountPoint, err)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Mounting %s to %s...", actualDeviceName, mountPoint)
	if _, err := s.runCommand(ctx, "sudo", "mount", actualDeviceName, mountPoint); err != nil {
		return nil, fmt.Errorf("failed to mount %s to %s: %w", actualDeviceName, mountPoint, err)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Device %s mounted to %s.", actualDeviceName, mountPoint)

	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		s.logger.Info().Msgf("RestoreSnapshot: Starting docker service...")
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "start", "docker"); err != nil {
			return nil, fmt.Errorf("failed to start docker after mounting: %w", err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Docker service started.")

		s.logger.Info().Msgf("RestoreSnapshot: Displaying docker disk usage...")
		if _, err := s.runCommand(ctx, "sudo", "docker", "system", "info"); err != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: failed to display docker info: %v. Docker snapshot may not be working so unmounting docker folder.", err)
			// Try to unmount docker folder on error
			if _, err := s.runCommand(ctx, "sudo", "umount", mountPoint); err != nil {
				s.logger.Warn().Msgf("RestoreSnapshot: failed to unmount docker folder: %v", err)
			}
			return nil, fmt.Errorf("failed to display docker disk usage: %w", err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Docker disk usage displayed.")
	}

	return &RestoreSnapshotOutput{VolumeID: *newVolume.VolumeId, DeviceName: actualDeviceName, NewVolume: volumeIsNewAndUnformatted}, nil
}

// restoreSnapshotWindows handles Windows-specific volume mounting
func (s *AWSSnapshotter) restoreSnapshotWindows(ctx context.Context, newVolume *types.Volume, deviceName string, mountPoint string, volumeIsNewAndUnformatted bool) (*RestoreSnapshotOutput, error) {
	// On Windows, deviceName is like /dev/xvdf or /dev/nvme1n1, but we need to find the disk number
	// Convert device name to Windows disk number using PowerShell
	s.logger.Info().Msgf("RestoreSnapshot: Finding Windows disk for device %s...", deviceName)

	// Extract disk identifier from device name (e.g., xvdf -> f, nvme1n1 -> 1)
	// For Windows, we'll use PowerShell to find the disk by matching the device path
	// The device path in EC2 Windows instances maps to disk numbers

	// Use PowerShell to get disk information
	// First, wait a bit for the disk to appear in Windows
	time.Sleep(2 * time.Second)

	// Get all disks and find the one that matches our volume
	// We'll use Get-Disk and match by size or by checking which disk is new
	psScript := `
		$disks = Get-Disk | Where-Object { $_.OperationalStatus -eq 'Offline' -or $_.PartitionStyle -eq 'Raw' }
		if ($disks) {
			$disk = $disks | Select-Object -First 1
			Write-Output $disk.Number
		} else {
			# Try to find by checking recently attached disks
			$allDisks = Get-Disk | Sort-Object Number
			$disk = $allDisks | Where-Object { $_.Number -ne 0 } | Select-Object -First 1
			if ($disk) {
				Write-Output $disk.Number
			} else {
				Write-Error "No suitable disk found"
			}
		}
	`

	diskNumOutput, err := s.runCommand(ctx, "powershell", "-Command", psScript)
	if err != nil {
		return nil, fmt.Errorf("failed to find Windows disk: %w", err)
	}
	diskNumber := strings.TrimSpace(string(diskNumOutput))
	s.logger.Info().Msgf("RestoreSnapshot: Found Windows disk number: %s", diskNumber)

	// Extract drive letter from mountPoint (e.g., "C:\test-volume" -> "C", or "E:" -> "E")
	driveLetter := ""
	if len(mountPoint) >= 2 && mountPoint[1] == ':' {
		driveLetter = strings.ToUpper(string(mountPoint[0])) + ":"
	} else {
		// If mountPoint is a path, extract the drive letter or assign a new one
		// For Windows, we'll assign the next available drive letter starting from E:
		psScript = `
			$drives = Get-PSDrive -PSProvider FileSystem | Where-Object { $_.Name -match '^[A-Z]$' } | Select-Object -ExpandProperty Name
			$available = 'E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z' | Where-Object { $drives -notcontains $_ }
			if ($available) {
				Write-Output ($available | Select-Object -First 1)
			} else {
				Write-Error "No available drive letter"
			}
		`
		driveOutput, err := s.runCommand(ctx, "powershell", "-Command", psScript)
		if err != nil {
			return nil, fmt.Errorf("failed to find available drive letter: %w", err)
		}
		driveLetter = strings.TrimSpace(string(driveOutput)) + ":"
		s.logger.Info().Msgf("RestoreSnapshot: Assigned drive letter: %s", driveLetter)
	}

	// Save volume info before formatting/mounting
	volumeInfo := &VolumeInfo{
		VolumeID:   *newVolume.VolumeId,
		DeviceName: fmt.Sprintf("\\\\.\\PhysicalDrive%s", diskNumber),
		MountPoint: driveLetter + "\\",
		NewVolume:  volumeIsNewAndUnformatted,
	}
	if err := s.saveVolumeInfo(volumeInfo); err != nil {
		s.logger.Warn().Msgf("RestoreSnapshot: Failed to save volume info: %v", err)
	}

	if volumeIsNewAndUnformatted {
		s.logger.Info().Msgf("RestoreSnapshot: Initializing and formatting disk %s with NTFS...", diskNumber)
		// Initialize disk, create partition, format with NTFS, and assign drive letter
		psScript = fmt.Sprintf(`
			Initialize-Disk -Number %s -PartitionStyle GPT -Confirm:$false
			$partition = New-Partition -DiskNumber %s -UseMaximumSize -AssignDriveLetter
			Format-Volume -Partition $partition -FileSystem NTFS -Confirm:$false -Force
			$partition | Set-Partition -NewDriveLetter '%c'
			Write-Output "Disk formatted and drive letter assigned"
		`, diskNumber, diskNumber, driveLetter[0])

		if _, err := s.runCommand(ctx, "powershell", "-Command", psScript); err != nil {
			return nil, fmt.Errorf("failed to format disk %s: %w", diskNumber, err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Disk %s formatted with NTFS and assigned drive letter %s.", diskNumber, driveLetter)
	} else {
		// Volume from snapshot - just assign drive letter to existing partition
		s.logger.Info().Msgf("RestoreSnapshot: Assigning drive letter %s to disk %s...", driveLetter, diskNumber)
		psScript = fmt.Sprintf(`
			$disk = Get-Disk -Number %s
			if ($disk.PartitionStyle -eq 'Raw') {
				Initialize-Disk -Number %s -PartitionStyle GPT -Confirm:$false
				$partition = New-Partition -DiskNumber %s -UseMaximumSize -AssignDriveLetter
				Format-Volume -Partition $partition -FileSystem NTFS -Confirm:$false -Force
			}
			$partition = Get-Partition -DiskNumber %s | Where-Object { $_.Type -ne 'Reserved' } | Select-Object -First 1
			if ($partition) {
				$partition | Set-Partition -NewDriveLetter '%c'
				Write-Output "Drive letter assigned"
			} else {
				Write-Error "No partition found on disk"
			}
		`, diskNumber, diskNumber, diskNumber, diskNumber, driveLetter[0])

		if _, err := s.runCommand(ctx, "powershell", "-Command", psScript); err != nil {
			return nil, fmt.Errorf("failed to assign drive letter to disk %s: %w", diskNumber, err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Drive letter %s assigned to disk %s.", driveLetter, diskNumber)
	}

	// Update mountPoint to use the drive letter
	if !strings.HasSuffix(mountPoint, "\\") && !strings.HasSuffix(mountPoint, "/") {
		mountPoint = driveLetter + "\\"
	} else {
		mountPoint = driveLetter + "\\"
	}

	return &RestoreSnapshotOutput{VolumeID: *newVolume.VolumeId, DeviceName: fmt.Sprintf("\\\\.\\PhysicalDrive%s", diskNumber), NewVolume: volumeIsNewAndUnformatted}, nil
}

func replaceFilterValues(filters []types.Filter, name string, values []string) error {
	for i, filter := range filters {
		if *filter.Name == name {
			filters[i].Values = values
			return nil
		}
	}

	return fmt.Errorf("filter %s not found in filters: %v", name, utils.PrettyPrint(filters))
}
