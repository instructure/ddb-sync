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

package status_test

import (
	"fmt"
	"testing"

	"github.com/instructure/ddb-sync/status"
)

var (
	Empty        = status.EmptyBufferState
	Quarter      = status.FillBufferStates[0]
	Half         = status.FillBufferStates[1]
	ThreeQuarter = status.FillBufferStates[2]
	Full         = status.FillBufferStates[3]
)

const cap = 100

type BufferTestCase struct {
	fill         int
	expectedFill string
}

func TestBufferStatus(t *testing.T) {

	testCases := []*BufferTestCase{
		&BufferTestCase{
			fill:         0,
			expectedFill: Empty,
		},
		&BufferTestCase{
			fill:         1,
			expectedFill: Quarter,
		},
		&BufferTestCase{
			fill:         25,
			expectedFill: Quarter,
		},
		&BufferTestCase{
			fill:         26,
			expectedFill: Half,
		},
		&BufferTestCase{
			fill:         50,
			expectedFill: Half,
		},
		&BufferTestCase{
			fill:         75,
			expectedFill: ThreeQuarter,
		},
		&BufferTestCase{
			fill:         99,
			expectedFill: Full,
		},
		&BufferTestCase{
			fill:         100,
			expectedFill: Full,
		},
		&BufferTestCase{
			fill:         100000000,
			expectedFill: Full,
		},
	}

	for _, testCase := range testCases {
		bufState := status.BufferStatus(testCase.fill, cap)

		if bufState != fmt.Sprintf("⇨ %s ⇨", testCase.expectedFill) {
			t.Errorf("Expected %q for %d/%d: returned %q", testCase.expectedFill, testCase.fill, cap, bufState)
		}
	}
}
