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

package utils_test

import (
	"testing"
	"time"

	"github.com/instructure/ddb-sync/utils"
)

type formatDurationTestCase struct {
	duration time.Duration
	expected string
}

func TestFormatDuration(t *testing.T) {
	testCases := []formatDurationTestCase{
		formatDurationTestCase{
			duration: time.Nanosecond + time.Second,
			expected: "1s",
		},
		formatDurationTestCase{
			duration: time.Millisecond + time.Second,
			expected: "1s",
		},
		formatDurationTestCase{
			duration: time.Minute,
			expected: "1m0s",
		},
		formatDurationTestCase{
			duration: time.Minute + time.Second,
			expected: "1m1s",
		},
		formatDurationTestCase{
			duration: 12*time.Hour + 30*time.Minute + time.Second,
			expected: "12h30m",
		},
		formatDurationTestCase{
			duration: 36 * time.Hour,
			expected: "36h0m",
		},
	}

	for _, testCase := range testCases {
		result := utils.FormatDuration(testCase.duration)
		if testCase.expected != result {
			t.Errorf("%q didn't match the expected output: %q", result, testCase.expected)
		}
	}
}
