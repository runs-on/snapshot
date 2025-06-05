package snapshot

import (
	"fmt"
	"path/filepath"
	"strings"
)

// getVolumeInfoPath returns the path to the volume info JSON file for a given mount point
func getVolumeInfoPath(mountPoint string) string {
	// Replace slashes with hyphens and remove leading/trailing hyphens
	sanitizedPath := strings.Trim(strings.ReplaceAll(mountPoint, "/", "-"), "-")
	return filepath.Join("/runs-on", fmt.Sprintf("snapshot-%s.json", sanitizedPath))
}
