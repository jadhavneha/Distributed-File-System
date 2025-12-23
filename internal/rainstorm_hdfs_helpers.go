// rainstorm_hdfs_helpers.go
package main

import (
	"fmt"
	"os"
	"strings"
)

func hdfsGet(hdfsName, localPath string) error {
	return DownloadHDFSFileForRainStorm(hdfsName, localPath, "")
}

func hdfsAppend(localPath, hdfsName string) error {
	if err := AppendLocalFileToHDFS(localPath, hdfsName); err != nil {
		if !strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("hdfsAppend: append failed for %s: %w", hdfsName, err)
		}

		tmp, tmpErr := os.CreateTemp("", "rainstorm-empty-*")
		if tmpErr != nil {
			return fmt.Errorf("hdfsAppend: failed to create temp file: %w", tmpErr)
		}
		tmpPath := tmp.Name()
		tmp.Close()
		defer os.Remove(tmpPath)

		if err2 := CreateFileOnHDFS(tmpPath, hdfsName); err2 != nil {
			if !strings.Contains(err2.Error(), "already existed") {
				return fmt.Errorf("hdfsAppend: create failed for %s: %w", hdfsName, err2)
			}
		}

		if err3 := AppendLocalFileToHDFS(localPath, hdfsName); err3 != nil {
			return fmt.Errorf("hdfsAppend: append-after-create failed for %s: %w", hdfsName, err3)
		}
	}

	return nil
}
