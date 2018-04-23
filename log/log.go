package log

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	logger      = log.New(os.Stdout, "", log.LstdFlags)
	statusLock  sync.Mutex
	statusLines []string
)

// ANSI helpers

const CSI = "\033["

func EraseLineAfterCursor() {
	fmt.Fprintf(os.Stderr, "%s%dK", CSI, 0)
}

func MoveToColumn(col int) {
	fmt.Fprintf(os.Stderr, "%s%dG", CSI, col)
}

func MoveToPreviousLine(count int) {
	fmt.Fprintf(os.Stderr, "%s%dF", CSI, count)
}

// END ANSI helpers

func ClearStatus() {
	statusLock.Lock()
	defer statusLock.Unlock()

	hideStatus()
	statusLines = statusLines[:0]
}

func hideStatus() {
	MoveToColumn(0)
	EraseLineAfterCursor()
	for i := 0; i < len(statusLines)-1; i++ {
		MoveToPreviousLine(0)
		EraseLineAfterCursor()
	}
}

func showStatus() {
	for i, line := range statusLines {
		if i < len(statusLines)-1 {
			fmt.Fprintln(os.Stdout, line)
		} else {
			fmt.Fprint(os.Stdout, line)
		}
	}
}

func StatusPrint(v ...interface{}) {
	statusPrint(fmt.Sprint(v...))
}

func StatusPrintf(format string, v ...interface{}) {
	statusPrint(fmt.Sprintf(format, v...))
}

func StatusPrintln(v ...interface{}) {
	statusPrint(fmt.Sprintln(v...))
}

func statusPrint(s string) {
	statusLock.Lock()
	defer statusLock.Unlock()

	fmt.Fprint(os.Stdout, s)

	sLines := strings.Split(s, "\n")
	if len(statusLines) == 0 {
		statusLines = sLines
	} else {
		statusLines[len(statusLines)-1] += sLines[0]
		statusLines = append(statusLines, sLines[1:]...)
	}
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
