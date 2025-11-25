package snapshot

import (
	"context"
	"fmt"
	"strconv"
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

	baseFilters := s.baseSnapshotFilters()
	s.logger.Info().Msgf("RestoreSnapshot: Base filters: %s", utils.PrettyPrint(baseFilters))

	latestSnapshot, err := s.findSnapshotByKeyCandidates(ctx, baseFilters)
	if err != nil {
		return nil, err
	}

	if latestSnapshot == nil {
		s.logger.Info().Msgf("RestoreSnapshot: No snapshot found via key search, falling back to branch %s", gitBranch)
		latestSnapshot, err = s.findSnapshotByBranch(ctx, baseFilters, gitBranch)
		if err != nil {
			return nil, err
		}
		if latestSnapshot != nil {
			s.logger.Info().Msgf("RestoreSnapshot: Found snapshot %s for branch %s", *latestSnapshot.SnapshotId, gitBranch)
		}
	}

	if latestSnapshot == nil && s.config.RunnerConfig.DefaultBranch != "" {
		defaultBranch := s.config.RunnerConfig.DefaultBranch
		s.logger.Info().Msgf("RestoreSnapshot: No snapshot found for branch %s, trying default branch %s", gitBranch, defaultBranch)
		latestSnapshot, err = s.findSnapshotByBranch(ctx, baseFilters, defaultBranch)
		if err != nil {
			return nil, err
		}
		if latestSnapshot != nil {
			s.logger.Info().Msgf("RestoreSnapshot: Found snapshot %s from default branch %s", *latestSnapshot.SnapshotId, defaultBranch)
		}
	}

	if latestSnapshot == nil {
		if s.config.RunnerConfig.DefaultBranch != "" {
			s.logger.Info().Msgf("RestoreSnapshot: No existing snapshot found for branch %s or default branch %s. A new volume will be created.", gitBranch, s.config.RunnerConfig.DefaultBranch)
		} else {
			s.logger.Info().Msgf("RestoreSnapshot: No existing snapshot found for branch %s. A new volume will be created.", gitBranch)
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

// restoreSnapshotWindows handles Windows-specific volume mounting using AWS's documented approach
func (s *AWSSnapshotter) restoreSnapshotWindows(ctx context.Context, newVolume *types.Volume, deviceName string, mountPoint string, volumeIsNewAndUnformatted bool) (*RestoreSnapshotOutput, error) {
	s.logger.Info().Msgf("RestoreSnapshot: Preparing Windows disk for volume %s...", *newVolume.VolumeId)

	// Wait for disk to appear in Windows
	time.Sleep(3 * time.Second)

	// Normalize the requested mount path
	windowsMountPoint := strings.ReplaceAll(mountPoint, "/", "\\")
	windowsMountPoint = strings.TrimSpace(windowsMountPoint)
	for strings.Contains(windowsMountPoint, "\\\\") {
		windowsMountPoint = strings.ReplaceAll(windowsMountPoint, "\\\\", "\\")
	}
	if len(windowsMountPoint) < 2 || windowsMountPoint[1] != ':' {
		return nil, fmt.Errorf("invalid Windows path '%s'. Expected a path like C:\\data", mountPoint)
	}

	// Determine if we're mounting to a drive letter or a folder path
	isDriveLetter := false
	driveLetter := strings.ToUpper(string(windowsMountPoint[0]))
	targetPath := windowsMountPoint
	if len(windowsMountPoint) == 2 {
		isDriveLetter = true
		targetPath = fmt.Sprintf("%s:\\", driveLetter)
	} else if len(windowsMountPoint) == 3 && (windowsMountPoint[2] == '\\' || windowsMountPoint[2] == '/') {
		isDriveLetter = true
		targetPath = fmt.Sprintf("%s:\\", driveLetter)
	} else {
		targetPath = strings.TrimRight(targetPath, "\\")
	}

	// First, list all disks for debugging
	s.logger.Info().Msgf("RestoreSnapshot: Listing all available disks...")
	listDisksScript := `
		Get-Disk | Select-Object Number, PartitionStyle, OperationalStatus, Size, FriendlyName | Format-Table -AutoSize | Out-String
	`
	listOutput, listErr := s.runCommand(ctx, "powershell", "-Command", listDisksScript)
	if listErr == nil {
		s.logger.Info().Msgf("Available disks:\n%s", string(listOutput))
	} else {
		s.logger.Warn().Msgf("Failed to list disks: %v", listErr)
	}

	// Get expected volume size in bytes for matching GPT disks
	expectedSizeBytes := int64(s.config.VolumeSize) * 1024 * 1024 * 1024 // Convert GiB to bytes
	if newVolume.Size != nil {
		expectedSizeBytes = int64(*newVolume.Size) * 1024 * 1024 * 1024
	}

	var diskNumber, partitionNumber string
	var isNewVolume bool

	// First, try to find and initialize a raw disk (new volume)
	s.logger.Info().Msgf("RestoreSnapshot: Checking for raw disk (new volume)...")
	psScript := `
		$ErrorActionPreference = 'Stop'
		Stop-Service -Name ShellHWDetection
		try {
			Write-Host "Searching for raw disks..."
			$allDisks = Get-Disk | Select-Object Number, PartitionStyle, OperationalStatus, Size, FriendlyName
			Write-Host "All disks found:"
			$allDisks | Format-Table -AutoSize | Out-String | Write-Host
			
			$disk = Get-Disk | Where-Object { $_.PartitionStyle -eq 'raw' } | Select-Object -First 1
			if (-not $disk) {
				Write-Host "No raw disk found"
				exit 0
			}
			$diskNumber = $disk.Number
			Write-Host "Found raw disk: $diskNumber (Size: $($disk.Size), Status: $($disk.OperationalStatus))"
			
			Write-Host "Initializing disk $diskNumber..."
			Initialize-Disk -Number $diskNumber -PartitionStyle GPT -Confirm:$false
			
			Write-Host "Creating partition on disk $diskNumber..."
			$partition = New-Partition -DiskNumber $diskNumber -UseMaximumSize -AssignDriveLetter:$false
			
			Write-Host "Formatting partition $($partition.PartitionNumber) with NTFS..."
			Format-Volume -Partition $partition -FileSystem NTFS -Confirm:$false -Force | Out-Null
			
			Write-Host "Successfully initialized disk $diskNumber, partition $($partition.PartitionNumber)"
			Write-Output "$diskNumber,$($partition.PartitionNumber)"
		} catch {
			Write-Error "PowerShell error: $($_.Exception.Message)"
			Write-Error "Error details: $($_.Exception | Format-List -Force | Out-String)"
			exit 1
		} finally {
			Start-Service -Name ShellHWDetection
		}
	`

	output, err := s.runCommand(ctx, "powershell", "-Command", psScript)
	if err == nil && len(output) > 0 {
		// Successfully initialized raw disk - parse output
		outputStr := strings.TrimSpace(string(output))
		lines := strings.Split(outputStr, "\n")
		var resultLine string
		for i := len(lines) - 1; i >= 0; i-- {
			line := strings.TrimSpace(lines[i])
			if parts := strings.Split(line, ","); len(parts) == 2 {
				if strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
					resultLine = line
					break
				}
			}
		}
		if resultLine != "" {
			parts := strings.Split(resultLine, ",")
			diskNumber = strings.TrimSpace(parts[0])
			partitionNumber = strings.TrimSpace(parts[1])
			isNewVolume = true
			s.logger.Info().Msgf("RestoreSnapshot: Initialized new raw disk %s, partition %s", diskNumber, partitionNumber)
		}
	}

	// If no raw disk found, look for GPT disk matching expected size (existing snapshot)
	if diskNumber == "" {
		s.logger.Info().Msgf("RestoreSnapshot: No raw disk found, looking for GPT disk matching size %d bytes...", expectedSizeBytes)
		psScript = fmt.Sprintf(`
			$ErrorActionPreference = 'Stop'
			$expectedSize = %d
			$tolerance = 104857600
			Write-Host "Searching for GPT disk matching size $expectedSize (tolerance: $tolerance bytes)..."
			
			$allDisks = Get-Disk | Select-Object Number, PartitionStyle, OperationalStatus, Size, FriendlyName
			Write-Host "All disks found:"
			$allDisks | Format-Table -AutoSize | Out-String | Write-Host
			
			$matchingDisks = Get-Disk | Where-Object { 
				$_.PartitionStyle -eq 'GPT' -and 
				$_.Number -ne 0 -and 
				[Math]::Abs($_.Size - $expectedSize) -lt $tolerance
			} | Sort-Object Number
			
			if (-not $matchingDisks) {
				Write-Error "No GPT disk found matching expected size $expectedSize"
				exit 1
			}
			
			$disk = $matchingDisks | Select-Object -First 1
			$diskNumber = $disk.Number
			Write-Host "Found matching GPT disk: $diskNumber (Size: $($disk.Size), Expected: $expectedSize)"
			
			$partition = Get-Partition -DiskNumber $diskNumber | Where-Object { $_.Type -ne 'Reserved' } | Select-Object -First 1
			if (-not $partition) {
				Write-Error "No partition found on disk $diskNumber"
				exit 1
			}
			
			Write-Host "Found partition $($partition.PartitionNumber) on disk $diskNumber"
			Write-Output "$diskNumber,$($partition.PartitionNumber)"
		`, expectedSizeBytes)

		output, err = s.runCommand(ctx, "powershell", "-Command", psScript)
		if err != nil {
			outputStr := string(output)
			if outputStr == "" {
				outputStr = "(no output)"
			}
			return nil, fmt.Errorf("failed to find GPT disk matching expected size. PowerShell output:\n%s\nError: %w", outputStr, err)
		}

		// Parse disk and partition numbers
		outputStr := strings.TrimSpace(string(output))
		lines := strings.Split(outputStr, "\n")
		var resultLine string
		for i := len(lines) - 1; i >= 0; i-- {
			line := strings.TrimSpace(lines[i])
			if parts := strings.Split(line, ","); len(parts) == 2 {
				if strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
					resultLine = line
					break
				}
			}
		}

		if resultLine == "" {
			return nil, fmt.Errorf("unexpected output from GPT disk search. Could not find disk/partition numbers. Full output:\n%s", outputStr)
		}

		parts := strings.Split(resultLine, ",")
		diskNumber = strings.TrimSpace(parts[0])
		partitionNumber = strings.TrimSpace(parts[1])
		isNewVolume = false
		s.logger.Info().Msgf("RestoreSnapshot: Found existing GPT disk %s, partition %s", diskNumber, partitionNumber)
	}

	// Mount the initialized disk to the requested path
	if isDriveLetter {
		s.logger.Info().Msgf("RestoreSnapshot: Assigning drive letter %s...", driveLetter)
		psScript = fmt.Sprintf(`
			Set-Partition -DiskNumber %s -PartitionNumber %s -NewDriveLetter '%s' -ErrorAction Stop
			Write-Output "Drive letter assigned"
		`, diskNumber, partitionNumber, driveLetter)
		if _, err := s.runCommand(ctx, "powershell", "-Command", psScript); err != nil {
			return nil, fmt.Errorf("failed to assign drive letter %s: %w", driveLetter, err)
		}
	} else {
		s.logger.Info().Msgf("RestoreSnapshot: Mounting to path %s...", targetPath)
		pathQuoted := strconv.Quote(targetPath)
		psScript = fmt.Sprintf(`
			New-Item -ItemType Directory -Path %s -Force | Out-Null
			Add-PartitionAccessPath -DiskNumber %s -PartitionNumber %s -AccessPath %s -ErrorAction Stop
			Write-Output "Access path added"
		`, pathQuoted, diskNumber, partitionNumber, pathQuoted)
		if _, err := s.runCommand(ctx, "powershell", "-Command", psScript); err != nil {
			return nil, fmt.Errorf("failed to mount at %s: %w", targetPath, err)
		}
	}

	volumeInfo := &VolumeInfo{
		VolumeID:   *newVolume.VolumeId,
		DeviceName: fmt.Sprintf("\\\\.\\PhysicalDrive%s", diskNumber),
		MountPoint: targetPath,
		NewVolume:  isNewVolume,
	}
	if err := s.saveVolumeInfo(volumeInfo); err != nil {
		s.logger.Warn().Msgf("RestoreSnapshot: Failed to save volume info: %v", err)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Successfully mounted volume to %s", targetPath)
	return &RestoreSnapshotOutput{VolumeID: *newVolume.VolumeId, DeviceName: fmt.Sprintf("\\\\.\\PhysicalDrive%s", diskNumber), NewVolume: isNewVolume}, nil
}

type keyCandidate struct {
	value  string
	prefix bool
}

func (s *AWSSnapshotter) findSnapshotByKeyCandidates(ctx context.Context, baseFilters []types.Filter) (*types.Snapshot, error) {
	candidates := make([]keyCandidate, 0, 1+len(s.config.RestoreKeys))
	seen := make(map[string]struct{})

	if s.config.SnapshotKey != "" {
		candidates = append(candidates, keyCandidate{value: s.config.SnapshotKey})
		seen[s.config.SnapshotKey] = struct{}{}
	}

	for _, restoreKey := range s.config.RestoreKeys {
		restoreKey = strings.TrimSpace(restoreKey)
		if restoreKey == "" {
			continue
		}
		if _, ok := seen[restoreKey]; ok {
			continue
		}
		candidates = append(candidates, keyCandidate{value: restoreKey, prefix: true})
		seen[restoreKey] = struct{}{}
	}

	for _, candidate := range candidates {
		snapshot, err := s.searchSnapshotByKey(ctx, baseFilters, candidate.value, candidate.prefix)
		if err != nil {
			return nil, err
		}
		if snapshot != nil {
			if candidate.prefix {
				s.logger.Info().Msgf("RestoreSnapshot: Found snapshot %s using restore key prefix %s", *snapshot.SnapshotId, candidate.value)
			} else {
				s.logger.Info().Msgf("RestoreSnapshot: Found snapshot %s using exact key %s", *snapshot.SnapshotId, candidate.value)
			}
			return snapshot, nil
		}
	}
	return nil, nil
}

func (s *AWSSnapshotter) searchSnapshotByKey(ctx context.Context, baseFilters []types.Filter, key string, prefix bool) (*types.Snapshot, error) {
	if key == "" {
		return nil, nil
	}
	filters := append([]types.Filter{}, baseFilters...)
	if !prefix {
		filters = append(filters, types.Filter{
			Name:   aws.String(fmt.Sprintf("tag:%s", snapshotTagKeyKey)),
			Values: []string{key},
		})
	}

	snapshots, err := s.describeSnapshotsWithFilters(ctx, filters)
	if err != nil {
		return nil, err
	}

	var latest *types.Snapshot
	for i := range snapshots {
		snap := &snapshots[i]
		tagValue := getTagValueByKey(snap.Tags, snapshotTagKeyKey)
		if tagValue == "" {
			continue
		}
		if prefix {
			if !strings.HasPrefix(tagValue, key) {
				continue
			}
		} else if tagValue != key {
			continue
		}
		if latest == nil || (snap.StartTime != nil && (latest.StartTime == nil || snap.StartTime.After(*latest.StartTime))) {
			latest = snap
		}
	}

	return latest, nil
}

func (s *AWSSnapshotter) findSnapshotByBranch(ctx context.Context, baseFilters []types.Filter, branch string) (*types.Snapshot, error) {
	if branch == "" {
		return nil, nil
	}

	filters := append([]types.Filter{}, baseFilters...)
	filters = append(filters, types.Filter{
		Name:   aws.String(fmt.Sprintf("tag:%s", snapshotTagKeyBranch)),
		Values: []string{branch},
	})

	snapshots, err := s.describeSnapshotsWithFilters(ctx, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to describe snapshots for branch %s: %w", branch, err)
	}

	return latestSnapshotFromList(snapshots), nil
}

func (s *AWSSnapshotter) describeSnapshotsWithFilters(ctx context.Context, filters []types.Filter) ([]types.Snapshot, error) {
	var allSnapshots []types.Snapshot
	input := &ec2.DescribeSnapshotsInput{
		Filters:  filters,
		OwnerIds: []string{"self"},
	}

	for {
		output, err := s.ec2Client.DescribeSnapshots(ctx, input)
		if err != nil {
			return nil, err
		}
		allSnapshots = append(allSnapshots, output.Snapshots...)
		if output.NextToken == nil {
			break
		}
		input.NextToken = output.NextToken
	}

	return allSnapshots, nil
}

func latestSnapshotFromList(snapshots []types.Snapshot) *types.Snapshot {
	var latest *types.Snapshot
	for i := range snapshots {
		snap := &snapshots[i]
		if snap.StartTime == nil {
			continue
		}
		if latest == nil || latest.StartTime == nil || snap.StartTime.After(*latest.StartTime) {
			latest = snap
		}
	}
	return latest
}

func getTagValueByKey(tags []types.Tag, key string) string {
	for _, tag := range tags {
		if tag.Key != nil && *tag.Key == key && tag.Value != nil {
			return *tag.Value
		}
	}
	return ""
}
