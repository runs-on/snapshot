package snapshot

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type restoreCandidate struct {
	branch string
	key    string
	source string
}

func (s *AWSSnapshotter) restoreCandidates() []restoreCandidate {
	keys := append([]string{s.config.Key}, s.config.RestoreKeys...)
	branches := []string{s.config.GithubRef}
	if s.config.DefaultBranchFallback && s.config.RunnerConfig != nil {
		branch := s.config.RunnerConfig.DefaultBranch
		if branch != "" && branch != s.config.GithubRef {
			branches = append(branches, branch)
		}
	}
	var candidates []restoreCandidate
	seen := make(map[[2]string]bool)
	for branchIndex, branch := range branches {
		for keyIndex, key := range keys {
			identity := [2]string{branch, key}
			if seen[identity] {
				continue
			}
			seen[identity] = true
			source := "branch"
			if keyIndex > 0 {
				source = "restore-key"
			}
			if branchIndex > 0 {
				source = "default-branch"
				if keyIndex > 0 {
					source += "-restore-key"
				}
			}
			candidates = append(candidates, restoreCandidate{branch, key, source})
		}
	}
	return candidates
}

func (s *AWSSnapshotter) lookupSnapshot(ctx context.Context, client ec2.DescribeSnapshotsAPIClient) (*types.Snapshot, restoreCandidate, error) {
	for _, candidate := range s.restoreCandidates() {
		filters := []types.Filter{{Name: aws.String("status"), Values: []string{"completed"}}}
		for _, tag := range s.defaultTags() {
			value := aws.ToString(tag.Value)
			switch aws.ToString(tag.Key) {
			case snapshotTagKeyVersion:
				value = s.snapshotVersion(candidate.key)
			case snapshotTagKeyBranch:
				value = candidate.branch
			}
			filters = append(filters, types.Filter{Name: aws.String("tag:" + aws.ToString(tag.Key)), Values: []string{value}})
		}
		pages := ec2.NewDescribeSnapshotsPaginator(client, &ec2.DescribeSnapshotsInput{Filters: filters, OwnerIds: []string{"self"}})
		var latest *types.Snapshot
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				return nil, candidate, fmt.Errorf("describe snapshot candidate %s: %w", candidate.source, err)
			}
			for _, snapshot := range page.Snapshots {
				if snapshot.SnapshotId == nil || snapshot.StartTime == nil || snapshot.VolumeSize == nil || *snapshot.VolumeSize < s.config.VolumeSize {
					continue
				}
				if latest == nil || snapshot.StartTime.After(*latest.StartTime) {
					copy := snapshot
					latest = &copy
				}
			}
		}
		if latest != nil {
			return latest, candidate, nil
		}
	}
	return nil, restoreCandidate{source: "empty"}, nil
}
