package log

import (
	"fmt"
	"log"
	"os"
	"sync"

	"gerrit.instructure.com/ddb-sync/status"
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
