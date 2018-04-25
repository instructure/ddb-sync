package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"time"

	"gerrit.instructure.com/ddb-sync/log"
)

var StopSignals = []os.Signal{
	os.Interrupt,
}

func main() {
	plans, err := ParseArgs(os.Args[1:])
	if err != nil {
		log.Printf("[ERROR] %v\n", err)
		os.Exit(1)
	}

	dispatcher, err := NewDispatcher(plans)
	if err != nil {
		log.Printf("[ERROR] %v\n", err)
		os.Exit(2)
	}

	dispatcher.Start()

	// Start the signal handler
	StartSignalHandler(dispatcher)

	// Start the display ticker
	displayTicker := StartDisplayTicker(dispatcher)

	// Wait for all operators to indicate completion
	err = dispatcher.Wait()
	displayTicker.Stop()

	displayStatus(dispatcher)

	switch err {
	case nil:
	case context.Canceled:
		log.Print("[USER CANCELED]\n")
		os.Exit(130)
	case ErrOperationFailed:
		log.Print("[OPERATION FAILED]\n")
		os.Exit(79)
	default:
		log.Printf("[ERROR] %v\n", err)
		os.Exit(79)
	}
}

func StartSignalHandler(dispatcher *Dispatcher) {
	go func() {
		sigs := make(chan os.Signal)
		signal.Notify(sigs, StopSignals...)

		<-sigs
		dispatcher.Cancel() // only signal Cancel() once

		signal.Ignore(StopSignals...)
	}()
}

func StartDisplayTicker(dispatcher *Dispatcher) *time.Ticker {
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		displayStatus(dispatcher)
		for range ticker.C {
			displayStatus(dispatcher)
		}
	}()
	return ticker
}

func displayStatus(dispatcher *Dispatcher) {
	statuses := dispatcher.Statuses()

	log.ClearStatus()
	log.StatusPrintln(strings.Join(statuses, "\n"))
}
