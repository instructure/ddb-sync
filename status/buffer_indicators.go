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

package status

import (
	"fmt"
	"math"
)

// BufferStates are single character "fullness" indicators of the buffer
var (
	EmptyBufferState = "○"
	FillBufferStates = []string{
		"◔",
		"◑",
		"◕",
		"●",
	}
)

func BufferStatus(fill, capacity int) string {
	state := EmptyBufferState

	// the empty state is used only when the fill is at 0
	if fill > 0 {
		if fill > capacity {
			fill = capacity
		}

		fillPercentage := float64(fill) / float64(capacity)
		selection := int(math.Ceil(fillPercentage*float64(len(FillBufferStates)))) - 1
		state = FillBufferStates[selection]
	}

	return fmt.Sprintf("⇨ %s ⇨", state)
}
