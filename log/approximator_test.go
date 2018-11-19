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

package log_test

import (
	"fmt"
	"testing"

	"github.com/instructure/ddb-sync/log"
)

func TestApproximate(t *testing.T) {
	testCase := func(num int, expectation string, t *testing.T) {
		result := log.Approximate(num)
		if result != expectation {
			t.Error(fmt.Errorf("failed: returned val %s != %s when given %d", result, expectation, num))
		}
	}

	testCase(1000, "~1k", t)
	testCase(1000000, "~1m", t)
	testCase(1000000000, "~1b", t)
	testCase(1000000000000, "~1t", t)
	testCase(1499, "~1k", t)
	testCase(1501, "~2k", t)
	testCase(6700, "~7k", t)
	testCase(10000, "~10k", t)
	testCase(100000, "~100k", t)
	testCase(12, "12", t)
	testCase(0, "0", t)
}
