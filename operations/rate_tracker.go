/*
 * ddb-sync
 * Copyright (C) 2018 Instructure Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package operations

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/instructure/ddb-sync/log"
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
