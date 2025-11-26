package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/runs-on/snapshot/internal/utils"
	"github.com/sethvargo/go-githubactions"
)

const requiredTagKey = "runs-on-stack-name"

// InputSource represents where inputs should be read from.
type InputSource int

const (
	// InputSourceWorkflow reads directly from workflow inputs (INPUT_* env vars).
	InputSourceWorkflow InputSource = iota
	// InputSourceState reads from action state captured during the main phase.
	InputSourceState
)

type inputFetcher func(string) string

func newInputFetcher(action *githubactions.Action, source InputSource) inputFetcher {
	return func(name string) string {
		switch source {
		case InputSourceState:
			return strings.TrimSpace(os.Getenv(stateEnvVar(name)))
		default:
			value := strings.TrimSpace(action.GetInput(name))
			saveInputState(action, name, value)
			return value
		}
	}
}

func saveInputState(action *githubactions.Action, name, value string) {
	action.SaveState(stateKeyForInput(name), value)
}

func stateEnvVar(name string) string {
	return fmt.Sprintf("STATE_%s", stateKeyForInput(name))
}

func stateKeyForInput(name string) string {
	return fmt.Sprintf("INPUT_%s", normalizeInputName(name))
}

var inputNameReplacer = strings.NewReplacer(" ", "_", "-", "_")

func normalizeInputName(name string) string {
	return strings.ToUpper(inputNameReplacer.Replace(name))
}

type Config struct {
	Path                     string
	Version                  string
	WaitForCompletion        bool
	Save                     bool
	VolumeType               types.VolumeType
	VolumeIops               int32
	VolumeThroughput         int32
	VolumeSize               int32
	VolumeInitializationRate int32
	VolumeName               string
	GithubRef                string
	GithubFullRef            string
	GithubRepository         string
	InstanceID               string
	Az                       string
	CustomTags               []Tag
	SnapshotName             string
	SnapshotKey              string
	RestoreKeys              []string
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

// NewConfigFromInputs parses workflow inputs and stores them in state for the post phase.
func NewConfigFromInputs(action *githubactions.Action) *Config {
	return newConfig(action, InputSourceWorkflow)
}

// NewConfigFromState reconstructs the config using values stored during the main phase.
func NewConfigFromState(action *githubactions.Action) *Config {
	return newConfig(action, InputSourceState)
}

func newConfig(action *githubactions.Action, source InputSource) *Config {
	cfg := &Config{
		GithubRef:        os.Getenv("GITHUB_REF_NAME"),
		GithubFullRef:    os.Getenv("GITHUB_REF"),
		GithubRepository: os.Getenv("GITHUB_REPOSITORY"),
		InstanceID:       os.Getenv("RUNS_ON_INSTANCE_ID"),
		Az:               os.Getenv("RUNS_ON_AWS_AZ"),
	}

	getInput := newInputFetcher(action, source)

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

	requiredTagPresent := false
	for _, tag := range cfg.RunnerConfig.CustomTags {
		if tag.Key == requiredTagKey {
			requiredTagPresent = true
		}
		cfg.CustomTags = append(cfg.CustomTags, Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}

	if !requiredTagPresent {
		action.Fatalf("Required tag '%s' is not present in the RunsOn config file.", requiredTagKey)
	}

	pathInput := getInput("path")
	cleanedPath, err := parseAndCleanPath(pathInput)
	if err != nil {
		action.Fatalf("%v", err)
	}
	cfg.Path = cleanedPath

	cfg.Version = strings.TrimSpace(getInput("version"))
	// Fallback to environment variable directly in case GetInput doesn't work
	if cfg.Version == "" {
		cfg.Version = strings.TrimSpace(os.Getenv("INPUT_VERSION"))
	}
	if cfg.Version == "" {
		cfg.Version = "v1"
	}

	cfg.WaitForCompletion = getInput("wait_for_completion") != "false"
	cfg.Save = getInput("save") != "false"

	rawKey := strings.TrimSpace(getInput("key"))
	if rawKey == "" {
		rawKey = defaultSnapshotKey(cfg.GithubRef, cfg.GithubFullRef)
	}
	cfg.SnapshotKey = rawKey

	cfg.RestoreKeys = parseRestoreKeys(getInput("restore-keys"))
	if len(cfg.RestoreKeys) == 0 {
		cfg.RestoreKeys = defaultRestoreKeys(cfg.GithubRef, cfg.RunnerConfig.DefaultBranch)
	}

	volumeType := getInput("volume_type")
	if volumeType == "" {
		volumeType = "gp3"
	}
	cfg.VolumeType = types.VolumeType(volumeType)

	cfg.VolumeInitializationRate = parseInt(action, getInput, "volume_initialization_rate", 0, 0)
	cfg.VolumeIops = parseInt(action, getInput, "volume_iops", 100, 0)
	cfg.VolumeThroughput = parseInt(action, getInput, "volume_throughput", 100, 0)
	cfg.VolumeSize = parseInt(action, getInput, "volume_size", 1, 0)

	action.Infof("Input 'path': %v", cfg.Path)
	action.Infof("Input 'version': %s", cfg.Version)
	action.Infof("Input 'wait_for_completion': %t", cfg.WaitForCompletion)

	return cfg
}

// parseAndCleanPath validates and cleans a path input.
// It trims whitespace, normalizes path separators, and ensures the path is absolute.
// Returns an error if the path is empty or not absolute.
func parseAndCleanPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("path is required")
	}
	// Normalize path separators (handles mixed \ and /)
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("path '%s' must be an absolute path", path)
	}
	return path, nil
}

func parseInt(action *githubactions.Action, fetch inputFetcher, input string, min int, max int) int32 {
	value := fetch(input)
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

func parseRestoreKeys(raw string) []string {
	if raw == "" {
		return nil
	}
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(raw, "\n")
	keys := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		keys = append(keys, line)
	}
	return keys
}

func defaultSnapshotKey(refName, fullRef string) string {
	if refName == "" && fullRef == "" {
		return ""
	}
	if refName == "" {
		return fullRef
	}
	if fullRef == "" {
		return refName
	}
	return fmt.Sprintf("%s-%s", refName, fullRef)
}

func defaultRestoreKeys(refName, defaultBranch string) []string {
	restoreKeys := make([]string, 0, 2)
	if refName != "" {
		restoreKeys = append(restoreKeys, fmt.Sprintf("%s-", refName))
	}
	if defaultBranch != "" {
		restoreKeys = append(restoreKeys, fmt.Sprintf("%s-", defaultBranch))
	}
	return restoreKeys
}
