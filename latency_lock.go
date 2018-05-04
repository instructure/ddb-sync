package main

import (
	"strings"
	"sync"
	"time"
)

type latencyLock struct {
	timestamp time.Time
	mux       sync.Mutex
}

func (l *latencyLock) Update(lastCheck time.Time) {
	l.mux.Lock()
	defer l.mux.Unlock()

	l.timestamp = lastCheck
}

func (l *latencyLock) String() string {
	l.mux.Lock()
	defer l.mux.Unlock()

	duration := time.Since(l.timestamp).Round(time.Minute).String()

	if strings.HasSuffix(duration, "m0s") {
		duration = duration[:len(duration)-2]
	}
	if strings.HasSuffix(duration, "h0m") {
		duration = duration[:len(duration)-2]
	}

	return duration
}
