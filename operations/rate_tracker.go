package operations

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RateTracker is a structure to hold and manage a rate tracking loop, counters and timers
type RateTracker struct {
	m sync.RWMutex

	rateTime   time.Time
	rateTicker *time.Ticker

	countAtLastReset int64
	counter          int64
	lastRate         float64
}

// Return a new RateTracker
func NewRateTracker(tickFreq time.Duration) *RateTracker {
	return &RateTracker{
		rateTime:   time.Now(),
		rateTicker: time.NewTicker(tickFreq),
	}
}

// Increment takes a value to add to the current counter
func (t *RateTracker) Increment(by int64) {
	atomic.AddInt64(&t.counter, by)
}

// Count returns the current count
func (t *RateTracker) Count() int64 {
	return atomic.LoadInt64(&t.counter)
}

// Start start the rate tracker
func (t *RateTracker) Start() {
	go func() {
		for range t.rateTicker.C {
			t.m.Lock()

			recordCount := t.Count() - atomic.LoadInt64(&t.countAtLastReset)
			// Set the rate from the concluding window
			t.lastRate = float64(recordCount) / time.Since(t.rateTime).Seconds()

			// Reset the window
			t.rateTime = time.Now()
			atomic.StoreInt64(&t.countAtLastReset, t.Count())

			t.m.Unlock()
		}
	}()
}

// Stop stop the rate tracker
func (t *RateTracker) Stop() {
	t.rateTicker.Stop()
}

// RecordsPerSecond returns a pretty formatted description of the rate from the last completed window
func (t *RateTracker) RecordsPerSecond() string {
	t.m.RLock()
	defer t.m.RUnlock()
	return fmt.Sprintf("%.f/s", t.lastRate)
}
