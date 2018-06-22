package status_test

import (
	"fmt"
	"testing"

	"gerrit.instructure.com/ddb-sync/status"
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
