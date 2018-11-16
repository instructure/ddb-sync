package operations

import (
	"fmt"
	"sync"
	"time"

	"github.com/instructure/ddb-sync/utils"
)

type LatencyLock struct {
	timestamp   time.Time
	mux         sync.RWMutex
	initialized bool
}

func (l *LatencyLock) Update(lastCheck time.Time) {
	l.mux.Lock()
	defer l.mux.Unlock()

	l.initialized = true
	l.timestamp = lastCheck
}

func (l *LatencyLock) Status() string {
	l.mux.RLock()
	defer l.mux.RUnlock()
	if !l.initialized {
		return "--"
	}

	duration := utils.FormatDuration(time.Since(l.timestamp))

	return fmt.Sprintf("~%s", duration)
}
