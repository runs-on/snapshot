package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/runs-on/snapshot/internal/utils"
	"github.com/sethvargo/go-githubactions"
)

type Config struct {
	Path                     string
	Version                  string
	WaitForCompletion        bool
	VolumeType               types.VolumeType
	VolumeIops               int32
	VolumeThroughput         int32
	VolumeSize               int32
	VolumeInitializationRate int32
	VolumeName               string
	GithubRef                string
	GithubRepository         string
	InstanceID               string
	Az                       string
	DefaultBranch            string
	CustomTags               []Tag
	SnapshotName             string
	RunnerConfig             *RunnerConfig
}

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RunnerConfig struct {
	DefaultBranch string `json:"defaultBranch"`
	CustomTags    []Tag  `json:"customTags"`
}

// NewConfigFromInputs parses action inputs and environment variables to build the Config struct.
func NewConfigFromInputs(action *githubactions.Action) *Config {
	cfg := &Config{
		GithubRef:        os.Getenv("GITHUB_REF_NAME"),
		GithubRepository: os.Getenv("GITHUB_REPOSITORY"),
		InstanceID:       os.Getenv("RUNS_ON_INSTANCE_ID"),
		Az:               os.Getenv("RUNS_ON_AWS_AZ"),
	}

	configBytes, err := os.ReadFile(filepath.Join(os.Getenv("RUNS_ON_HOME"), "config.json"))
	if err != nil {
		action.Fatalf("Error reading RunsOn config file: %v. You must be using RunsOn v2.8.3+", err)
	} else {
		var runnerConfig RunnerConfig
		if err := json.Unmarshal(configBytes, &runnerConfig); err != nil {
			action.Warningf("Error parsing RunsOn config file: %v", err)
		} else {
			cfg.RunnerConfig = &runnerConfig
			action.Infof("Runner config: %s", utils.PrettyPrint(cfg.RunnerConfig))
		}
	}

	for _, tag := range cfg.RunnerConfig.CustomTags {
		cfg.CustomTags = append(cfg.CustomTags, Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}

	path := action.GetInput("path")
	path = strings.TrimSpace(path)
	if path == "" {
		action.Fatalf("Path is required.")
	}
	if !strings.HasPrefix(path, "/") {
		action.Fatalf("Path '%s' must be an absolute path.", path)
	}
	cfg.Path = path

	cfg.Version = action.GetInput("version")
	if cfg.Version == "" {
		cfg.Version = "v1"
	}

	cfg.WaitForCompletion = action.GetInput("wait_for_completion") != "false"

	volumeType := action.GetInput("volume_type")
	if volumeType == "" {
		volumeType = "gp3"
	}
	cfg.VolumeType = types.VolumeType(volumeType)

	cfg.VolumeInitializationRate = parseInt(action, "volume_initialization_rate", 0, 0)
	cfg.VolumeIops = parseInt(action, "volume_iops", 100, 0)
	cfg.VolumeThroughput = parseInt(action, "volume_throughput", 100, 0)
	cfg.VolumeSize = parseInt(action, "volume_size", 1, 0)

	action.Infof("Input 'path': %v", cfg.Path)
	action.Infof("Input 'version': %s", cfg.Version)
	action.Infof("Input 'wait_for_completion': %t", cfg.WaitForCompletion)

	return cfg
}

func parseInt(action *githubactions.Action, input string, min int, max int) int32 {
	value := action.GetInput(input)
	if value == "" {
		action.Fatalf("%s' cannot be empty", input)
	}
	valueInt, err := strconv.Atoi(value)
	if err != nil {
		action.Fatalf("Invalid value '%s': %v", value, err)
	}
	if valueInt < min {
		action.Fatalf("Invalid value '%s': must be at least %d", value, min)
	}
	if max > 0 && valueInt > max {
		action.Fatalf("Invalid value '%s': must be at most %d", value, max)
	}
	return int32(valueInt)
}
