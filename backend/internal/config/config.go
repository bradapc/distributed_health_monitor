package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

//JSON parsing and config watcher

type rawTarget struct {
	URL      string `json:"url"`
	Interval string `json:"interval"`
	Timeout  string `json:"timeout"`
}

type MonitorTarget struct {
	URL      string
	Interval time.Duration
	Timeout  time.Duration
}

var tempTargetMutex sync.Mutex

// ReplaceFile atomically streams the user payload of a target json file change to a tmp file then atomically renames it to targets.json, allowing the hot reloader to parse changes
func ReplaceFile(targetsPayload []byte) error {
	tempTargetMutex.Lock()
	defer tempTargetMutex.Unlock()
	file, err := os.OpenFile("configs/targets.tmp", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = file.Write(targetsPayload)
	if err != nil {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}
	file.Close()
	err = os.Rename("configs/targets.tmp", "configs/targets.json")
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

// LoadTargets reads a filepath and converts the raw json to a slice of targets endpoints to be monitored
func LoadTargets(filename string) ([]MonitorTarget, error) {
	dataTargets, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error loading targets from json file: %w", err)
	}
	var rawTargets []rawTarget
	err = json.Unmarshal(dataTargets, &rawTargets)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling json targets: %w", err)
	}
	convertedTargets := make([]MonitorTarget, 0, len(rawTargets))
	for _, t := range rawTargets {
		ct, err := validateTarget(t)
		if err != nil {
			return nil, err
		}
		convertedTargets = append(convertedTargets, ct)
	}
	return convertedTargets, nil
}

// validateTarget reads a rawTarget from json, ensures it is an existing url with valid intervals, and converts it to a MonitorTarget
func validateTarget(target rawTarget) (MonitorTarget, error) {
	if target.URL == "" {
		return MonitorTarget{}, fmt.Errorf("invalid url on target: %s", target.URL)
	}
	inter, err := time.ParseDuration(target.Interval)
	if err != nil {
		return MonitorTarget{}, fmt.Errorf("target %s: invalid interval: %w", target.URL, err)
	}
	to, err := time.ParseDuration(target.Timeout)
	if err != nil {
		return MonitorTarget{}, fmt.Errorf("target %s: invalid timeout: %w", target.URL, err)
	}
	if to > inter {
		return MonitorTarget{}, fmt.Errorf("target %s: timeout cannot be greater than interval", target.URL)
	}

	var monitorTarget MonitorTarget = MonitorTarget{
		URL:      target.URL,
		Interval: inter,
		Timeout:  to,
	}
	return monitorTarget, nil
}
