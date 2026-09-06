package snapshot

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	runsOnConfig "github.com/runs-on/snapshot/internal/config"
)

type snapshotLookupFunc func(*ec2.DescribeSnapshotsInput) (*ec2.DescribeSnapshotsOutput, error)

func (f snapshotLookupFunc) DescribeSnapshots(_ context.Context, input *ec2.DescribeSnapshotsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	return f(input)
}

func TestKeyedIdentityAndRestoreOrder(t *testing.T) {
	s := &AWSSnapshotter{config: &runsOnConfig.Config{
		Path: "/mnt/build", Key: "a", Version: "v1", GithubRef: "feature",
		RestoreKeys: []string{"b", "b"}, DefaultBranchFallback: true,
		RunnerConfig: &runsOnConfig.RunnerConfig{DefaultBranch: "main"},
	}}
	if got := s.snapshotVersion(""); got != "v1" {
		t.Fatalf("unkeyed legacy identity changed: %q", got)
	}
	first := s.snapshotVersion("a")
	if first == s.snapshotVersion("b") {
		t.Fatal("matrix keys share an identity")
	}
	s.config.Path = "/mnt/build/../build/"
	if first != s.snapshotVersion("a") {
		t.Fatal("normalized paths should share identity")
	}
	s.config.Path = "/mnt/other"
	if first == s.snapshotVersion("a") {
		t.Fatal("different mount paths share an identity")
	}
	s.config.Path = "/mnt/build"
	s.config.Version = "v2"
	if first == s.snapshotVersion("a") {
		t.Fatal("manual version did not invalidate identity")
	}
	want := []restoreCandidate{{"feature", "a", "branch"}, {"feature", "b", "restore-key"}, {"main", "a", "default-branch"}, {"main", "b", "default-branch-restore-key"}}
	if got := s.restoreCandidates(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restore order = %#v, want %#v", got, want)
	}
	s.config.DefaultBranchFallback = false
	if got := s.restoreCandidates(); !reflect.DeepEqual(got, want[:2]) {
		t.Fatalf("disabled default-branch fallback: %#v", got)
	}
}

func TestLookupPaginatesAndKeepsRepositoryAndStackScope(t *testing.T) {
	s := &AWSSnapshotter{config: &runsOnConfig.Config{
		Path: "/mnt/build", Key: "primary", Version: "v1", VolumeSize: 20,
		GithubRef: "feature", GithubRepository: "owner/repo", RestoreKeys: []string{"fallback"},
		CustomTags: []runsOnConfig.Tag{{Key: "runs-on-stack-name", Value: "test-stack"}},
	}}
	old := time.Unix(100, 0)
	newer := time.Unix(200, 0)
	calls := 0
	client := snapshotLookupFunc(func(input *ec2.DescribeSnapshotsInput) (*ec2.DescribeSnapshotsOutput, error) {
		calls++
		filters := map[string]string{}
		for _, filter := range input.Filters {
			filters[aws.ToString(filter.Name)] = filter.Values[0]
		}
		for key, want := range map[string]string{
			"status": "completed", "tag:" + snapshotTagKeyRepository: "owner/repo",
			"tag:runs-on-stack-name": "test-stack", "tag:" + snapshotTagKeyBranch: "feature",
		} {
			if got := filters[key]; got != want {
				t.Errorf("filter %s = %q, want %q", key, got, want)
			}
		}
		if !reflect.DeepEqual(input.OwnerIds, []string{"self"}) {
			t.Error("snapshot lookup must remain account-scoped")
		}
		if filters["tag:"+snapshotTagKeyVersion] == s.snapshotVersion("primary") {
			return &ec2.DescribeSnapshotsOutput{Snapshots: []types.Snapshot{{SnapshotId: aws.String("too-small"), StartTime: &newer, VolumeSize: aws.Int32(10)}}}, nil
		}
		if input.NextToken == nil {
			return &ec2.DescribeSnapshotsOutput{NextToken: aws.String("page-2"), Snapshots: []types.Snapshot{{SnapshotId: aws.String("old"), StartTime: &old, VolumeSize: aws.Int32(20)}}}, nil
		}
		return &ec2.DescribeSnapshotsOutput{Snapshots: []types.Snapshot{{}, {SnapshotId: aws.String("new"), StartTime: &newer, VolumeSize: aws.Int32(20)}}}, nil
	})
	snapshot, candidate, err := s.lookupSnapshot(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || aws.ToString(snapshot.SnapshotId) != "new" || candidate.source != "restore-key" || calls != 3 {
		t.Fatalf("unexpected selection: snapshot=%v candidate=%#v calls=%d", snapshot, candidate, calls)
	}
	_, _, err = s.lookupSnapshot(context.Background(), snapshotLookupFunc(func(*ec2.DescribeSnapshotsInput) (*ec2.DescribeSnapshotsOutput, error) {
		return nil, errors.New("access denied")
	}))
	if err == nil {
		t.Fatal("AWS errors must not become a blank-volume cache miss")
	}
}
