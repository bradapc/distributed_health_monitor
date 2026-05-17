package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
)

var syntaxErr *json.SyntaxError

type FilePoller struct {
	filename         string
	lastModifiedTime time.Time
	pollTime         time.Duration
	d                *Dispatcher
}

func NewPoller(filename string, pollTime time.Duration, d *Dispatcher) *FilePoller {
	return &FilePoller{
		filename: filename,
		pollTime: pollTime,
		d:        d,
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
				err := fp.hotReload(ctx)
				if err != nil && errors.As(err, &syntaxErr) {
					fmt.Printf("WARNING: hot reload failed, check %s for syntax errors\n", fp.filename)
				} else if err != nil {
					fmt.Println(err)
				}
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

func (fp *FilePoller) hotReload(ctx context.Context) error {
	newTargets, err := config.LoadTargets(fp.filename)
	if err != nil {
		return fmt.Errorf("error loading new targets from hot refresh: %w", err)
	}
	err = fp.d.ReloadTargets(newTargets, ctx)
	if err != nil {
		return err
	}
	return nil
}
