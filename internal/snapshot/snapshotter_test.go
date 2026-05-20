package snapshot

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	runsOnConfig "github.com/runs-on/snapshot/internal/config"
)

func TestDefaultTagsIncludesSnapshotRepositoryAndRepoFullName(t *testing.T) {
	const repository = "owner/repo"

	snapshotter := &AWSSnapshotter{
		config: &runsOnConfig.Config{
			Version:          "v1",
			GithubRepository: repository,
			GithubRef:        "main",
		},
	}

	tags := tagsByKey(snapshotter.defaultTags())

	for _, key := range []string{snapshotTagKeyRepository, repoFullNameTagKey} {
		if got := tags[key]; got != repository {
			t.Fatalf("expected tag %s to be %q, got %q", key, repository, got)
		}
	}
}

func tagsByKey(tags []types.Tag) map[string]string {
	result := make(map[string]string, len(tags))
	for _, tag := range tags {
		if tag.Key == nil || tag.Value == nil {
			continue
		}
		result[*tag.Key] = *tag.Value
	}
	return result
}
