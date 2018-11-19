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
	"log"
	"os"
	"sync"

	"github.com/instructure/ddb-sync/status"
)

var (
	stdErrInteractive = false
	logger            = log.New(os.Stdout, "", log.LstdFlags)
	statusLineCount   = 0
	statusSet         = status.NewBlankSet()

	statusLock sync.Mutex
)

func InteractiveMode() bool {
	fi, err := os.Stderr.Stat()
	if err == nil {
		if fi.Mode()&os.ModeCharDevice == os.ModeCharDevice {
			stdErrInteractive = true
			return true
		}
	}
	stdErrInteractive = false
	return false
}

// ANSI helpers

const CSI = "\033["

func EraseLineAfterCursor() {
	fmt.Fprintf(os.Stderr, "%s%dK", CSI, 0)
}

func MoveToColumn(col int) {
	fmt.Fprintf(os.Stderr, "%s%dG", CSI, col)
}

func MoveCursorUp(count int) {
	fmt.Fprintf(os.Stderr, "%s%dA", CSI, count)
}

// END ANSI helpers

func ClearStatus() {
	statusLock.Lock()
	defer statusLock.Unlock()

	hideStatus()
}

func hideStatus() {
	if stdErrInteractive {
		MoveToColumn(1)
		EraseLineAfterCursor()
		for i := 0; i < statusLineCount; i++ {
			MoveCursorUp(1)
			MoveToColumn(1)
			EraseLineAfterCursor()
		}
	}
}

func showStatus() {
	if stdErrInteractive {
		lines := statusSet.Display()
		statusLineCount = len(lines)
		for _, line := range statusSet.Display() {
			fmt.Fprintln(os.Stderr, line)
		}
	} else {
		for _, line := range statusSet.ToFile() {
			fmt.Fprintln(os.Stderr, line)
		}
	}
}

func StatusPrint(set *status.Set) {
	statusSet = set
	statusPrint()
}

func statusPrint() {
	statusLock.Lock()
	defer statusLock.Unlock()

	showStatus()
}

func Print(v ...interface{}) {
	statusLock.Lock()
	defer statusLock.Unlock()

	if stdErrInteractive {
		hideStatus()
		defer showStatus()
	}
	logger.Print(v...)
}

func Printf(format string, v ...interface{}) {
	statusLock.Lock()
	defer statusLock.Unlock()

	if stdErrInteractive {
		hideStatus()
		defer showStatus()
	}
	logger.Printf(format, v...)
}

func Println(v ...interface{}) {
	statusLock.Lock()
	defer statusLock.Unlock()

	if stdErrInteractive {
		hideStatus()
		defer showStatus()
	}
	logger.Println(v...)
}
