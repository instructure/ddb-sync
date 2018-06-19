package operations

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

var DurationOutputFormat = regexp.MustCompile(`\d{1,2}\D`)

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

	duration := l.FormatDuration(time.Since(l.timestamp))

	return fmt.Sprintf("~%s", duration)
}

func (l *LatencyLock) FormatDuration(duration time.Duration) string {
	var str string
	switch {
	case duration > time.Hour:
		str = duration.Round(time.Minute).String()
	case duration > time.Minute:
		str = duration.Round(time.Second).String()
	case duration > time.Second:
		str = duration.Round(time.Second).String()
	}
	return strings.Join(DurationOutputFormat.FindAllString(str, 2), "")
}
