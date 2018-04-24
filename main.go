package main

import (
	"os"
	"os/signal"
	"strings"
	"time"

	"gerrit.instructure.com/ddb-sync/log"
	"gerrit.instructure.com/ddb-sync/plan"
)

var StopSignals = []os.Signal{
	os.Interrupt,
}

var plans = []plan.Plan{}

func main() {
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
	errs := dispatcher.Wait()
	displayTicker.Stop()

	if len(errs) > 0 {
		for _, err := range errs {
			log.Printf("[ERROR] %v\n", err)
		}

		os.Exit(5)
	}
}

func StartSignalHandler(dispatcher *Dispatcher) {
	go func() {
		sigs := make(chan os.Signal)
		signal.Notify(sigs, StopSignals...)

		<-sigs
		dispatcher.Stop() // only signal Stop() once

		signal.Ignore(StopSignals...)
	}()
}

func StartDisplayTicker(dispatcher *Dispatcher) *time.Ticker {
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			statuses := dispatcher.Statuses()

			log.ClearStatus()
			log.StatusPrintln(strings.Join(statuses, "\n"))
		}
	}()
	return ticker
}
