package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/sethvargo/go-githubactions"
)

func TestParseRestoreKeys(t *testing.T) {
	input := "foo-\nbar-\n\nbaz-\r\n"
	expected := []string{"foo-", "bar-", "baz-"}

	if got := parseRestoreKeys(input); !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}

	if got := parseRestoreKeys(""); got != nil {
		t.Fatalf("expected nil for empty input, got %v", got)
	}
}

func TestDefaultSnapshotKey(t *testing.T) {
	tests := []struct {
		name     string
		refName  string
		fullRef  string
		expected string
	}{
		{"both present", "main", "refs/heads/main", "main-refs/heads/main"},
		{"only ref name", "main", "", "main"},
		{"only full ref", "", "refs/tags/v1", "refs/tags/v1"},
		{"none", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultSnapshotKey(tt.refName, tt.fullRef); got != tt.expected {
				t.Fatalf("expected %s, got %s", tt.expected, got)
			}
		})
	}
}

func TestDefaultRestoreKeys(t *testing.T) {
	tests := []struct {
		name          string
		refName       string
		defaultBranch string
		expected      []string
	}{
		{"both present", "feature", "main", []string{"feature-", "main-"}},
		{"only ref name", "feature", "", []string{"feature-"}},
		{"only default", "", "main", []string{"main-"}},
		{"none", "", "", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := defaultRestoreKeys(tt.refName, tt.defaultBranch)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Fatalf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestParseAndCleanPath(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		expected    string
		description string
	}{
		{
			name:        "normal unix absolute path",
			input:       "/home/user/data",
			expectError: false,
			expected:    "/home/user/data",
			description: "Simple Unix absolute path should remain unchanged",
		},
		{
			name:        "specific case from issue",
			input:       "/home/runner/_work/foresight-premier/foresight-premier/Library",
			expectError: false,
			expected:    "/home/runner/_work/foresight-premier/foresight-premier/Library",
			description: "Long path with repeated directory names should not be truncated",
		},
		{
			name:        "path with trailing slash",
			input:       "/home/user/data/",
			expectError: false,
			expected:    "/home/user/data",
			description: "Trailing slash should be removed",
		},
		{
			name:        "path with extra spaces",
			input:       "  /home/user/data  ",
			expectError: false,
			expected:    "/home/user/data",
			description: "Leading and trailing spaces should be trimmed",
		},
		{
			name:        "path with dot components",
			input:       "/home/user/./data",
			expectError: false,
			expected:    "/home/user/data",
			description: "Dot components should be cleaned",
		},
		{
			name:        "path with double dot components",
			input:       "/home/user/../user/data",
			expectError: false,
			expected:    "/home/user/data",
			description: "Double dot components should be resolved",
		},
		{
			name:        "path with multiple slashes",
			input:       "/home//user///data",
			expectError: false,
			expected:    "/home/user/data",
			description: "Multiple consecutive slashes should be normalized",
		},
		{
			name:        "empty path",
			input:       "",
			expectError: true,
			expected:    "",
			description: "Empty path should return error",
		},
		{
			name:        "whitespace only path",
			input:       "   ",
			expectError: true,
			expected:    "",
			description: "Whitespace-only path should return error",
		},
		{
			name:        "relative path",
			input:       "relative/path",
			expectError: true,
			expected:    "",
			description: "Relative path should return error",
		},
		{
			name:        "relative path with dot",
			input:       "./relative/path",
			expectError: true,
			expected:    "",
			description: "Relative path starting with dot should return error",
		},
		{
			name:        "root path",
			input:       "/",
			expectError: false,
			expected:    "/",
			description: "Root path should be valid",
		},
		{
			name:        "deep nested path",
			input:       "/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p",
			expectError: false,
			expected:    "/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p",
			description: "Very deep nested paths should not be truncated",
		},
		{
			name:        "path with special characters",
			input:       "/home/user/my-data_123/test",
			expectError: false,
			expected:    "/home/user/my-data_123/test",
			description: "Paths with underscores and numbers should be preserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseAndCleanPath(tt.input)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error for input '%s', but got none. Result: '%s'", tt.input, result)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error for input '%s': %v", tt.input, err)
				}
				if result != tt.expected {
					t.Fatalf("expected '%s', got '%s' (input: '%s')", tt.expected, result, tt.input)
				}
			}
		})
	}
}

func TestParseAndCleanPathWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific tests skipped on non-Windows platform")
	}

	tests := []struct {
		name        string
		input       string
		expectError bool
		expected    string
		description string
	}{
		{
			name:        "windows absolute path with backslash",
			input:       "C:\\Users\\data",
			expectError: false,
			expected:    "C:\\Users\\data",
			description: "Windows path with backslashes should be cleaned",
		},
		{
			name:        "windows absolute path with forward slash",
			input:       "C:/Users/data",
			expectError: false,
			expected:    "C:\\Users\\data",
			description: "Windows path with forward slashes should be normalized to backslashes",
		},
		{
			name:        "windows path with mixed separators",
			input:       "C:\\Users/data\\test",
			expectError: false,
			expected:    "C:\\Users\\data\\test",
			description: "Windows path with mixed separators should be normalized",
		},
		{
			name:        "windows path with trailing backslash",
			input:       "C:\\Users\\data\\",
			expectError: false,
			expected:    "C:\\Users\\data",
			description: "Trailing backslash should be removed",
		},
		{
			name:        "windows drive root",
			input:       "C:\\",
			expectError: false,
			expected:    "C:\\",
			description: "Windows drive root should be valid",
		},
		{
			name:        "windows UNC path",
			input:       "\\\\server\\share\\path",
			expectError: false,
			expected:    "\\\\server\\share\\path",
			description: "UNC paths should be preserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseAndCleanPath(tt.input)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error for input '%s', but got none. Result: '%s'", tt.input, result)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error for input '%s': %v", tt.input, err)
				}
				// On Windows, filepath.Clean may normalize separators, so we compare cleaned versions
				expectedClean := filepath.Clean(tt.expected)
				resultClean := filepath.Clean(result)
				if resultClean != expectedClean {
					t.Fatalf("expected '%s' (cleaned: '%s'), got '%s' (cleaned: '%s') (input: '%s')",
						tt.expected, expectedClean, result, resultClean, tt.input)
				}
			}
		})
	}
}

func TestParseAndCleanPathUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific tests skipped on Windows platform")
	}

	tests := []struct {
		name        string
		input       string
		expectError bool
		expected    string
		description string
	}{
		{
			name:        "unix path with backslash",
			input:       "/home/user\\data",
			expectError: false,
			expected:    "/home/user\\data",
			description: "Unix path with backslash - backslashes are valid filename characters on Unix",
		},
		{
			name:        "unix path preserves forward slashes",
			input:       "/home/user/data",
			expectError: false,
			expected:    "/home/user/data",
			description: "Unix paths should preserve forward slashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseAndCleanPath(tt.input)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error for input '%s', but got none. Result: '%s'", tt.input, result)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error for input '%s': %v", tt.input, err)
				}
				if result != tt.expected {
					t.Fatalf("expected '%s', got '%s' (input: '%s')", tt.expected, result, tt.input)
				}
			}
		})
	}
}

func TestParseAndCleanPathNoTruncation(t *testing.T) {
	// This test specifically ensures that long paths don't get truncated
	// The specific case from the issue: /home/runner/_work/foresight-premier/foresight-premier/Library
	testCases := []string{
		"/home/runner/_work/foresight-premier/foresight-premier/Library",
		"/very/long/path/with/many/directories/that/should/not/be/truncated",
		"/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z",
		"/home/user/project/subproject/module/src/main/resources/config",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			result, err := parseAndCleanPath(tc)
			if err != nil {
				t.Fatalf("unexpected error for path '%s': %v", tc, err)
			}
			if result != tc {
				t.Fatalf("path was modified unexpectedly. Expected '%s', got '%s'", tc, result)
			}
			// Ensure the path wasn't truncated
			if len(result) < len(tc) {
				t.Fatalf("path appears to be truncated. Original length: %d, result length: %d", len(tc), len(result))
			}
		})
	}
}

func TestParseAndCleanPathErrorMessages(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errorSubstr string
	}{
		{
			name:        "empty path error",
			input:       "",
			errorSubstr: "path is required",
		},
		{
			name:        "relative path error",
			input:       "relative/path",
			errorSubstr: "must be an absolute path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseAndCleanPath(tt.input)
			if err == nil {
				t.Fatalf("expected error containing '%s', but got none", tt.errorSubstr)
			}
			// Verify error message contains expected text
			if err.Error() != "" && tt.errorSubstr != "" {
				if !strings.Contains(err.Error(), tt.errorSubstr) {
					t.Fatalf("error message '%s' does not contain expected substring '%s'", err.Error(), tt.errorSubstr)
				}
			}
		})
	}
}

func TestNewConfigFromState(t *testing.T) {
	tDir := t.TempDir()
	configPath := filepath.Join(tDir, "config.json")
	runnerCfg := RunnerConfig{
		DefaultBranch: "develop",
		CustomTags: []Tag{
			{Key: requiredTagKey, Value: "stack"},
			{Key: "env", Value: "test"},
		},
	}
	raw, err := json.Marshal(runnerCfg)
	if err != nil {
		t.Fatalf("failed to marshal runner config: %v", err)
	}
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	path := filepath.Join(tDir, "data")
	stateVars := map[string]string{
		"STATE_INPUT_PATH":                       path,
		"STATE_INPUT_VERSION":                    "v42",
		"STATE_INPUT_WAIT_FOR_COMPLETION":        "true",
		"STATE_INPUT_SAVE":                       "false",
		"STATE_INPUT_KEY":                        "custom-key",
		"STATE_INPUT_RESTORE_KEYS":               "foo-\nbar-",
		"STATE_INPUT_VOLUME_TYPE":                "io2",
		"STATE_INPUT_VOLUME_INITIALIZATION_RATE": "200",
		"STATE_INPUT_VOLUME_IOPS":                "7000",
		"STATE_INPUT_VOLUME_THROUGHPUT":          "1000",
		"STATE_INPUT_VOLUME_SIZE":                "99",
	}
	for k, v := range stateVars {
		t.Setenv(k, v)
	}
	t.Setenv("RUNS_ON_HOME", tDir)
	t.Setenv("RUNS_ON_INSTANCE_ID", "i-123")
	t.Setenv("RUNS_ON_AWS_AZ", "us-east-1a")
	t.Setenv("GITHUB_REF_NAME", "feature")
	t.Setenv("GITHUB_REF", "refs/heads/feature")
	t.Setenv("GITHUB_REPOSITORY", "runs-on/snapshot")

	action := githubactions.New()
	cfg := NewConfigFromState(action)

	if cfg.Path != filepath.Clean(path) {
		t.Fatalf("expected path %s, got %s", filepath.Clean(path), cfg.Path)
	}
	if cfg.Version != "v42" {
		t.Fatalf("expected version v42, got %s", cfg.Version)
	}
	if !cfg.WaitForCompletion {
		t.Fatalf("expected wait_for_completion true")
	}
	if cfg.Save {
		t.Fatalf("expected save false")
	}
	if cfg.SnapshotKey != "custom-key" {
		t.Fatalf("expected snapshot key custom-key, got %s", cfg.SnapshotKey)
	}
	if !reflect.DeepEqual(cfg.RestoreKeys, []string{"foo-", "bar-"}) {
		t.Fatalf("unexpected restore keys: %v", cfg.RestoreKeys)
	}
	if cfg.VolumeType != "io2" {
		t.Fatalf("unexpected volume type %s", cfg.VolumeType)
	}
	if cfg.VolumeInitializationRate != 200 {
		t.Fatalf("unexpected init rate %d", cfg.VolumeInitializationRate)
	}
	if cfg.VolumeIops != 7000 {
		t.Fatalf("unexpected iops %d", cfg.VolumeIops)
	}
	if cfg.VolumeThroughput != 1000 {
		t.Fatalf("unexpected throughput %d", cfg.VolumeThroughput)
	}
	if cfg.VolumeSize != 99 {
		t.Fatalf("unexpected volume size %d", cfg.VolumeSize)
	}
	if len(cfg.CustomTags) != 2 {
		t.Fatalf("expected 2 custom tags, got %d", len(cfg.CustomTags))
	}
}
