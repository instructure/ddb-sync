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
	prefix := "~"
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
		prefix = ""
		suffix = ""
		value = math.Round(realNum / u)
	default:
		prefix = ""
		suffix = ""
		value = float64(num)
	}

	return fmt.Sprintf("%s%.f%s", prefix, value, suffix)
}
