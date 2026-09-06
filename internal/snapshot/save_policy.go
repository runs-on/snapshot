package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type sourceMetadata struct {
	Schema     int    `json:"schema"`
	SourceSHA  string `json:"source_sha"`
	PolicyHash string `json:"policy_hash"`
}

type saveDecision struct {
	save     bool
	reason   string
	metadata sourceMetadata
}

func sourceMetadataPath(mountPoint string) string {
	return filepath.Join(mountPoint, ".runs-on-snapshot", "source.json")
}

func resolveCommit(ctx context.Context, repository, ref string) (string, error) {
	if ref == "" {
		return "", errors.New("missing source ref")
	}
	output, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}").Output()
	return strings.TrimSpace(string(output)), err
}

func (s *AWSSnapshotter) decideSave(ctx context.Context, mountPoint string) saveDecision {
	decision := saveDecision{save: true, reason: "save-enabled", metadata: sourceMetadata{Schema: 1}}
	if s.config.SaveMode == "false" {
		decision.save, decision.reason = false, "save-disabled"
		return decision
	}
	if s.config.SaveMode != "auto" || s.config.SaveIf != "git-paths-changed" {
		return decision
	}
	policy, _ := json.Marshal([]any{s.config.Version, s.config.Key, s.config.GitPaths})
	decision.metadata.PolicyHash = fmt.Sprintf("%x", sha256.Sum256(policy))
	head, err := resolveCommit(ctx, s.config.GitRepository, s.config.GitHead)
	checkout, checkoutErr := resolveCommit(ctx, s.config.GitRepository, "HEAD")
	if err != nil || checkoutErr != nil || head != checkout {
		decision.reason = "source-unavailable-or-checkout-mismatch"
		return decision
	}
	decision.metadata.SourceSHA = head
	if s.config.ForceSave {
		decision.reason = "force-save"
		return decision
	}
	var previous sourceMetadata
	data, err := readSourceMetadata(mountPoint)
	if err != nil || json.Unmarshal(data, &previous) != nil || previous.Schema != 1 || previous.SourceSHA == "" {
		decision.reason = "missing-source-metadata"
		return decision
	}
	if previous.PolicyHash != decision.metadata.PolicyHash {
		decision.reason = "different-stream-or-policy"
		return decision
	}
	base, err := resolveCommit(ctx, s.config.GitRepository, previous.SourceSHA)
	if err != nil {
		decision.reason = "base-commit-unavailable"
		return decision
	}
	if base == head {
		decision.save, decision.reason = false, "same-source-commit"
		return decision
	}
	args := []string{"-C", s.config.GitRepository, "diff", "--quiet", "--no-ext-diff", "--no-textconv", base, head, "--"}
	args = append(args, s.config.GitPaths...)
	err = exec.CommandContext(ctx, "git", args...).Run()
	if err == nil {
		decision.save, decision.reason = false, "no-relevant-path-changes"
		return decision
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
		decision.reason = "relevant-paths-changed"
	} else {
		decision.reason = "git-diff-unavailable"
	}
	return decision
}

func writeSourceMetadata(mountPoint string, metadata sourceMetadata) error {
	root, err := os.OpenRoot(mountPoint)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Mkdir(".runs-on-snapshot", 0755); err != nil && !os.IsExist(err) {
		return err
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	file, err := root.OpenFile(".runs-on-snapshot/source.json", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close())
}

func readSourceMetadata(mountPoint string) ([]byte, error) {
	root, err := os.OpenRoot(mountPoint)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.Open(".runs-on-snapshot/source.json")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// Treat oversized or malformed restored metadata as an uncertain base.
	return io.ReadAll(io.LimitReader(file, 4096))
}
