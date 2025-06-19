package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/rs/zerolog"
	runsOnConfig "github.com/runs-on/snapshot/internal/config"
)

const (
	// Tags used for resource identification
	snapshotTagKeyArch       = "runs-on-snapshot-arch"
	snapshotTagKeyPlatform   = "runs-on-snapshot-platform"
	snapshotTagKeyBranch     = "runs-on-snapshot-branch"
	snapshotTagKeyRepository = "runs-on-snapshot-repository"
	snapshotTagKeyVersion    = "runs-on-snapshot-version"
	nameTagKey               = "Name"
	timestampTagKey          = "runs-on-timestamp"
	ttlTagKey                = "runs-on-delete-after"

	suggestedDeviceName                 = "/dev/sdf" // AWS might assign /dev/xvdf etc.
	defaultVolumeInUseMaxWaitTime       = 5 * time.Minute
	defaultVolumeAvailableMaxWaitTime   = 5 * time.Minute
	defaultSnapshotCompletedMaxWaitTime = 10 * time.Minute
)

var defaultSnapshotCompletedWaiterOptions = func(o *ec2.SnapshotCompletedWaiterOptions) {
	o.MaxDelay = 3 * time.Second
	o.MinDelay = 3 * time.Second
}

var defaultVolumeInUseWaiterOptions = func(o *ec2.VolumeInUseWaiterOptions) {
	o.MaxDelay = 3 * time.Second
	o.MinDelay = 3 * time.Second
}

var defaultVolumeAvailableWaiterOptions = func(o *ec2.VolumeAvailableWaiterOptions) {
	o.MaxDelay = 3 * time.Second
	o.MinDelay = 3 * time.Second
}

// Snapshotter interface from the original file - kept for reference
type Snapshotter interface {
	CreateSnapshot(ctx context.Context, snapshot *Snapshot) error
	GetSnapshot(ctx context.Context, id string) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, id string) error
}

// AWSSnapshotter provides methods to manage EBS snapshots and volumes.
type AWSSnapshotter struct {
	logger    *zerolog.Logger
	config    *runsOnConfig.Config
	ec2Client *ec2.Client
}

// Snapshot struct from the original file - kept for reference, but not directly used by new funcs
type Snapshot struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RestoreSnapshotOutput holds the results of RestoreSnapshot.
type RestoreSnapshotOutput struct {
	VolumeID   string
	DeviceName string
	NewVolume  bool
}

// CreateSnapshotOutput holds the results of CreateSnapshot.
type CreateSnapshotOutput struct {
	SnapshotID string
}

// VolumeInfo stores information about the mounted volume
type VolumeInfo struct {
	VolumeID     string `json:"volume_id"`
	DeviceName   string `json:"device_name"`
	MountPoint   string `json:"mount_point"`
	AttachmentID string `json:"attachment_id,omitempty"`
	NewVolume    bool   `json:"new_volume,omitempty"`
}

// NewAWSSnapshotter creates a new AWSSnapshotter instance.
// It initializes the AWS SDK configuration and fetches EC2 instance metadata.
func NewAWSSnapshotter(ctx context.Context, logger *zerolog.Logger, cfg *runsOnConfig.Config) (*AWSSnapshotter, error) {
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS SDK config: %w", err)
	}

	if cfg.InstanceID == "" {
		return nil, fmt.Errorf("instanceID is required")
	}

	if cfg.Az == "" {
		return nil, fmt.Errorf("az is required")
	}

	if cfg.GithubRepository == "" {
		return nil, fmt.Errorf("githubRepository is required")
	}

	if cfg.GithubRef == "" {
		return nil, fmt.Errorf("githubRef is required")
	}

	if cfg.CustomTags == nil {
		cfg.CustomTags = []runsOnConfig.Tag{}
	}

	// we're currently using GITHUB_REF_NAME, so refs/ is not present, but just in case
	// https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/accessing-contextual-information-about-workflow-runs
	sanitizedGithubRef := strings.TrimPrefix(cfg.GithubRef, "refs/")
	sanitizedGithubRef = strings.ReplaceAll(sanitizedGithubRef, "/", "-")
	if len(sanitizedGithubRef) > 40 {
		sanitizedGithubRef = sanitizedGithubRef[:40]
	}

	currentTime := time.Now()
	if cfg.SnapshotName == "" {
		cfg.SnapshotName = fmt.Sprintf("runs-on-snapshot-%s-%s", sanitizedGithubRef, currentTime.Format("20060102-150405"))
	}

	if cfg.VolumeName == "" {
		cfg.VolumeName = fmt.Sprintf("runs-on-volume-%s-%s", sanitizedGithubRef, currentTime.Format("20060102-150405"))
	}

	return &AWSSnapshotter{
		logger:    logger,
		config:    cfg,
		ec2Client: ec2.NewFromConfig(awsConfig),
	}, nil
}

func (s *AWSSnapshotter) arch() string {
	return runtime.GOARCH
}

func (s *AWSSnapshotter) platform() string {
	return runtime.GOOS
}

func (s *AWSSnapshotter) defaultTags() []types.Tag {
	tags := []types.Tag{
		{Key: aws.String(snapshotTagKeyVersion), Value: aws.String(s.config.Version)},
		{Key: aws.String(snapshotTagKeyRepository), Value: aws.String(s.config.GithubRepository)},
		{Key: aws.String(snapshotTagKeyBranch), Value: aws.String(s.getSnapshotTagValue())},
		{Key: aws.String(snapshotTagKeyArch), Value: aws.String(s.arch())},
		{Key: aws.String(snapshotTagKeyPlatform), Value: aws.String(s.platform())},
	}
	for _, tag := range s.config.CustomTags {
		tags = append(tags, types.Tag{Key: aws.String(tag.Key), Value: aws.String(tag.Value)})
	}
	return tags
}

// saveVolumeInfo writes volume information to a JSON file
func (s *AWSSnapshotter) saveVolumeInfo(volumeInfo *VolumeInfo) error {
	infoPath := getVolumeInfoPath(volumeInfo.MountPoint)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(infoPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for volume info: %w", err)
	}

	data, err := json.MarshalIndent(volumeInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal volume info: %w", err)
	}

	if err := os.WriteFile(infoPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write volume info file: %w", err)
	}

	return nil
}

// loadVolumeInfo reads volume information from a JSON file
func (s *AWSSnapshotter) loadVolumeInfo(mountPoint string) (*VolumeInfo, error) {
	infoPath := getVolumeInfoPath(mountPoint)
	data, err := os.ReadFile(infoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read volume info file: %w", err)
	}

	var volumeInfo VolumeInfo
	if err := json.Unmarshal(data, &volumeInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal volume info: %w", err)
	}

	return &volumeInfo, nil
}

func (s *AWSSnapshotter) getSnapshotTagValue() string {
	return fmt.Sprintf("%s", s.config.GithubRef)
}

func (s *AWSSnapshotter) getSnapshotTagValueDefaultBranch() string {
	return fmt.Sprintf("%s", s.config.RunnerConfig.DefaultBranch)
}

// runCommand executes a shell command and returns its combined output or an error.
// It now requires a context for potential cancellation if the command runs too long.
func (s *AWSSnapshotter) runCommand(ctx context.Context, name string, arg ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, arg...)
	s.logger.Info().Msgf("Executing command: %s %s", name, strings.Join(arg, " "))
	output, err := cmd.CombinedOutput()
	if err != nil {
		s.logger.Warn().Msgf("Command failed: %s %s\nOutput:\n%s\nError: %v", name, strings.Join(arg, " "), string(output), err)
		return output, fmt.Errorf("command '%s %s' failed: %s: %w", name, strings.Join(arg, " "), string(output), err)
	}
	// Limit log output size for potentially verbose commands
	logOutput := string(output)
	if len(logOutput) > 400 {
		logOutput = logOutput[:200] + "... (output truncated)"
	}
	s.logger.Info().Msgf("Command successful. Output (first 200 chars or less):\n%s", logOutput)
	return output, nil
}

// getVolumeInfoPath returns the path to the volume info JSON file for a given mount point
func getVolumeInfoPath(mountPoint string) string {
	// Replace slashes with hyphens and remove leading/trailing hyphens
	sanitizedPath := strings.Trim(strings.ReplaceAll(mountPoint, "/", "-"), "-")
	return filepath.Join("/runs-on", fmt.Sprintf("snapshot-%s.json", sanitizedPath))
}
