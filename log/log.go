package log

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"text/tabwriter"
	"unicode/utf8"

	"gerrit.instructure.com/ddb-sync/status"
)

var (
	logger          = log.New(os.Stdout, "", log.LstdFlags)
	statusLock      sync.Mutex
	statusSet       *status.Set
	statusLineCount = 0
)

const (
	errorStatusLineCount   = 3
	minimumViewportWidth   = 80
	statusSquelchLenBuffer = 17
)

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
	MoveToColumn(1)
	EraseLineAfterCursor()
	for i := 0; i < statusLineCount-1; i++ {
		MoveCursorUp(1)
		MoveToColumn(1)
		EraseLineAfterCursor()
	}
}

func showStatus() {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	bufW := bufio.NewWriter(w) // buffered to 4096

	statusSet.UpdateViewport()
	statusLines := statusSet.Display()

	fmt.Fprintln(bufW, statusSet.Delimiter())
	fmt.Fprintln(bufW, statusSet.Header())
	seen := bufW.Buffered()
	statusLineCount = 2

	var calculatedLineLength int
	for i, line := range statusLines {
		statusLineCount++
		if i < len(statusLines)-1 {
			fmt.Fprintln(bufW, line)
		} else {
			fmt.Fprint(bufW, line)
		}

		runeCompensation := len(line) - utf8.RuneCountInString(line)
		numBytes := bufW.Buffered() - seen
		calculatedLineLength = numBytes - runeCompensation + statusSquelchLenBuffer
		if calculatedLineLength > statusSet.ViewportWidth || statusSet.ViewportWidth < minimumViewportWidth {
			bufW.Reset(w)
			fmt.Fprintln(bufW, statusSet.Delimiter())
			fmt.Fprintln(bufW, "Your terminal is too small for the status output.")
			fmt.Fprintf(bufW, "Status output disabled while terminal is too narrow.")
			statusLineCount = errorStatusLineCount
			break
		}
		seen = bufW.Buffered()
	}
	bufW.Flush()
	w.Flush()
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

	hideStatus()
	defer showStatus()
	logger.Print(v...)
}

func Printf(format string, v ...interface{}) {
	statusLock.Lock()
	defer statusLock.Unlock()

	hideStatus()
	defer showStatus()
	logger.Printf(format, v...)
}

func Println(v ...interface{}) {
	statusLock.Lock()
	defer statusLock.Unlock()

	hideStatus()
	defer showStatus()
	logger.Println(v...)
}
