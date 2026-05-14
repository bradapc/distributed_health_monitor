package monitor

import (
	"context"
	"net/http"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
)

//Per-URL loop logic and state machine

func RunWorker(ctx context.Context, target config.MonitorTarget, client *http.Client) {

}
