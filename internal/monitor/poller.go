package monitor

import (
	"context"
	"fmt"
	"os"
	"time"
)

type FilePoller struct {
	filename         string
	lastModifiedTime time.Time
	pollTime         time.Duration
}

func NewPoller(filename string, pollTime time.Duration) *FilePoller {
	return &FilePoller{
		filename: filename,
		pollTime: pollTime,
	}
}

func (fp *FilePoller) RunPoller(ctx context.Context) error {
	fileInfo, err := os.Stat(fp.filename)
	if err != nil {
		return fmt.Errorf("error initializing file poller: %w", err)
	}

	fp.lastModifiedTime = fileInfo.ModTime()
	ticker := time.NewTicker(fp.pollTime)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			changed, err := fp.PollFile(fp.filename)
			if err != nil {
				fmt.Println(err.Error())
			}
			if changed {
				fp.hotReload()
			}
		}
	}
}

func (fp *FilePoller) PollFile(filename string) (bool, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return false, fmt.Errorf("error polling file: %w", err)
	}
	if fileInfo.ModTime().After(fp.lastModifiedTime) {
		fp.lastModifiedTime = fileInfo.ModTime()
		return true, nil
	}
	return false, nil
}

func (fp *FilePoller) hotReload() error {
	fmt.Println("Detected refresh")
	return nil
}
