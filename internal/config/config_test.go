package config

import (
	"reflect"
	"testing"
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


