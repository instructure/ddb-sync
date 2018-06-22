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
