package log

import (
	"fmt"
	"math"
)

const (
	u float64 = 1
	k float64 = 1000 * u
	m float64 = 1000 * k
	b float64 = 1000 * m
	t float64 = 1000 * b
)

func Approximate(num int) string {
	var suffix string
	var value float64
	var realNum = float64(num)
	switch {
	case realNum >= t:
		suffix = "t"
		value = math.Round(realNum / t)
	case realNum >= b:
		suffix = "b"
		value = math.Round(realNum / b)
	case realNum >= m:
		suffix = "m"
		value = math.Round(realNum / m)
	case realNum >= k:
		suffix = "k"
		value = math.Round(realNum / k)
	case realNum >= u:
		suffix = "u"
		value = math.Round(realNum / u)

	}

	return fmt.Sprintf("~%.f%s", value, suffix)
}
