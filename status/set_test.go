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
	"testing"

	"github.com/instructure/ddb-sync/status"
)

func TestSetDelimiter(t *testing.T) {
	narrowSet := &status.Set{
		ViewportWidth: 20,
	}

	narrowTest := "- Current Status -"
	if narrowSet.Delimiter() != narrowTest {
		t.Errorf("@20 width: set didn't match\nTest   : %q\nPrinted: %q", narrowTest, narrowSet.Delimiter())
	}

	wideSet := &status.Set{
		ViewportWidth: 120,
	}

	wideTest := "--------------------------------------------------- Current Status ---------------------------------------------------"
	if wideSet.Delimiter() != wideTest {
		t.Errorf("@120 width: set didn't match\nTest   : %q\nPrinted: %q", wideTest, wideSet.Delimiter())
	}
}
