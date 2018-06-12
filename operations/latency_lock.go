package operations

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type latencyLock struct {
	timestamp   time.Time
	mux         sync.Mutex
	initialized bool
	checking    bool
}

func (l *latencyLock) Update(lastCheck time.Time) {
	l.mux.Lock()
	defer l.mux.Unlock()

	l.initialized = true
	l.timestamp = lastCheck
}

func (l *latencyLock) Status() string {
	l.mux.Lock()
	defer l.mux.Unlock()
	if !l.initialized {
		return "--"
	}

	duration := time.Since(l.timestamp).Round(time.Second).String()

	if strings.HasSuffix(duration, "m0s") {
		duration = duration[:len(duration)-2]
	}
	if strings.HasSuffix(duration, "h0m") {
		duration = duration[:len(duration)-2]
	}

	return fmt.Sprintf("~%s", duration)
}
