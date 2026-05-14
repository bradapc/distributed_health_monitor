package config

import (
	"encoding/json"
	"fmt"
	"os"
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
