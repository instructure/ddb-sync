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
