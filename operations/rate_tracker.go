package operations

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gerrit.instructure.com/ddb-sync/log"
)

// RateTracker is a structure to hold and manage a rate tracking loop, counters and timers
type RateTracker struct {
	m sync.RWMutex

	rateType string

	rateTicker *time.Ticker

	rateTime  time.Time
	startTime time.Time

	countAtLastReset int64
	counter          int64
	lastRate         float64
}

// Return a new RateTracker
func NewRateTracker(rateType string, tickFreq time.Duration) *RateTracker {
	return &RateTracker{
		rateType: rateType,

		rateTicker: time.NewTicker(tickFreq),

		rateTime:  time.Now(),
		startTime: time.Now(),
	}
}

// Increment takes a value to add to the current counter
func (t *RateTracker) Increment(by int64) {
	atomic.AddInt64(&t.counter, by)
}

// Count the absolute count of records we've written during the lifetime of the struct
func (t *RateTracker) Count() int64 {
	return atomic.LoadInt64(&t.counter)
}

// Start start the rate tracker
func (t *RateTracker) Start() {
	go func() {
		for range t.rateTicker.C {
			t.m.Lock()
			currentCount := t.Count()

			recordCount := currentCount - atomic.LoadInt64(&t.countAtLastReset)
			// Set the rate from the concluding window
			t.lastRate = float64(recordCount) / time.Since(t.rateTime).Seconds()

			// Reset the window
			t.rateTime = time.Now()
			atomic.StoreInt64(&t.countAtLastReset, currentCount)

			t.m.Unlock()
		}
	}()
}

// Stop stop the rate tracker
func (t *RateTracker) Stop() {
	t.rateTicker.Stop()
}

// ApproximateCount the approximate count of records we've written during the lifetime of the struct
func (t *RateTracker) ApproximateCount() string {
	return log.Approximate(int(t.Count()))
}

// RatePerSecond returns a pretty formatted description of the rate from the last completed window
func (t *RateTracker) RatePerSecond() string {
	t.m.RLock()
	defer t.m.RUnlock()
	return fmt.Sprintf("%.f %s/s", t.lastRate, t.rateType)
}

// Duration the duration since we started
func (t *RateTracker) Duration() time.Duration {
	return time.Since(t.startTime).Round(time.Second)
}
