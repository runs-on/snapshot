package snapshot

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	runsOnConfig "github.com/runs-on/snapshot/internal/config"
)

func TestSavePolicyUsesRelevantCommittedPaths(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repository := filepath.Join(root, "workspace")
	if err := os.MkdirAll(filepath.Join(repository, "src"), 0755); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		output, err := exec.Command("git", append([]string{"-C", repository}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	write := func(path, text string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(repository, path), []byte(text), 0644); err != nil {
			t.Fatal(err)
		}
	}
	commit := func() string {
		t.Helper()
		git("add", ".")
		git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", "commit", "-qm", "fixture")
		return git("rev-parse", "HEAD")
	}
	git("init", "-q")
	write("src/lib.rs", "pub fn example() {}\n")
	write("notes.md", "initial\n")
	base := commit()
	s := &AWSSnapshotter{config: &runsOnConfig.Config{
		SaveMode: "auto", SaveIf: "git-paths-changed", Key: "library", Version: "v1",
		GitRepository: repository, GitHead: base, GitPaths: []string{"src/**", "Cargo.lock"},
	}}
	assertDecision := func(wantSave bool, reason string) saveDecision {
		t.Helper()
		decision := s.decideSave(ctx, root)
		if decision.save != wantSave || decision.reason != reason {
			t.Fatalf("decision = %#v, want save=%t reason=%s", decision, wantSave, reason)
		}
		return decision
	}
	seed := assertDecision(true, "missing-source-metadata")
	if err := writeSourceMetadata(root, seed.metadata); err != nil {
		t.Fatal(err)
	}
	assertDecision(false, "same-source-commit")
	write("notes.md", "documentation change\n")
	s.config.GitHead = commit()
	assertDecision(false, "no-relevant-path-changes")
	write("src/lib.rs", "pub fn changed() {}\n")
	s.config.GitHead = commit()
	assertDecision(true, "relevant-paths-changed")
	s.config.GitHead = base
	if decision := assertDecision(true, "source-unavailable-or-checkout-mismatch"); decision.metadata.SourceSHA != "" {
		t.Fatal("mismatched checkout must not record an authoritative source SHA")
	}
	s.config.GitHead = git("rev-parse", "HEAD")
	s.config.Key = "other-matrix-entry"
	assertDecision(true, "different-stream-or-policy")
	s.config.Key = "library"
	s.config.ForceSave = true
	assertDecision(true, "force-save")
	s.config.SaveMode = "false"
	assertDecision(false, "save-disabled")
	s.config.SaveMode, s.config.ForceSave = "auto", false
	missing := seed.metadata
	missing.SourceSHA = strings.Repeat("0", 40)
	if err := writeSourceMetadata(root, missing); err != nil {
		t.Fatal(err)
	}
	assertDecision(true, "base-commit-unavailable")
	if err := os.WriteFile(sourceMetadataPath(root), []byte("broken metadata"), 0644); err != nil {
		t.Fatal(err)
	}
	assertDecision(true, "missing-source-metadata")
	// An invalid pathspec makes Git fail; uncertain comparisons must save.
	s.config.GitPaths = []string{":(invalid)src"}
	seed = s.decideSave(ctx, root)
	seed.metadata.SourceSHA = base
	if err := writeSourceMetadata(root, seed.metadata); err != nil {
		t.Fatal(err)
	}
	assertDecision(true, "git-diff-unavailable")
}

func TestSourceMetadataCannotEscapeTheSnapshotRoot(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	file := filepath.Join(outside, "source.json")
	if err := os.WriteFile(file, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, ".runs-on-snapshot")); err != nil {
		t.Fatal(err)
	}
	if err := writeSourceMetadata(root, sourceMetadata{Schema: 1}); err == nil {
		t.Fatal("metadata write escaped the snapshot root")
	}
	if _, err := readSourceMetadata(root); err == nil {
		t.Fatal("metadata read escaped the snapshot root")
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "keep" {
		t.Fatal("file outside snapshot changed")
	}
}
