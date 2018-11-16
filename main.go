package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/instructure/ddb-sync/log"
	"github.com/instructure/ddb-sync/operations"
)

var displayTickerTime = 15 * time.Second

const checkpointTickerTime = 20 * time.Minute

func init() {
	if log.InteractiveMode() {
		displayTickerTime = 500 * time.Millisecond
	}
}

var StopSignals = []os.Signal{
	os.Interrupt,
}

func main() {
	plan, err := ParseArgs(os.Args[1:])
	if err != nil {
		if err != ErrExit {
			fmt.Printf("[ERROR] %v\n", err)
		}
		os.Exit(1)
		return
	}

	dispatcher, err := NewDispatcher(plan)
	if err != nil {
		os.Exit(2)
		return
	}

	err = dispatcher.Preflights()
	if err != nil {
		os.Exit(2)
		return
	}

	displayStatus(dispatcher)

	// Start the signal handler
	StartSignalHandler(dispatcher)

	// Start the output tickers
	displayTicker := StartDisplayTicker(dispatcher)
	checkpointTicker := StartCheckpointTicker(dispatcher)

	err = dispatcher.Run()

	checkpointTicker.Stop()
	displayTicker.Stop()

	displayStatus(dispatcher)

	switch err {
	case nil:
	case context.Canceled:
		log.Print("[USER CANCELED]\n")
		fmt.Fprintf(os.Stderr, "[USER CANCELED]\n")
		os.Exit(130)
	case operations.ErrOperationFailed:
		log.Print("[OPERATION FAILED]\n")
		fmt.Fprintf(os.Stderr, "[OPERATION FAILED]\n")
		os.Exit(79)
	default:
		log.Printf("[ERROR] %v\n", err)
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
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
	ticker := time.NewTicker(displayTickerTime)
	go func() {
		displayStatus(dispatcher)
		for range ticker.C {
			displayStatus(dispatcher)
		}
	}()
	return ticker
}

func StartCheckpointTicker(dispatcher *Dispatcher) *time.Ticker {
	ticker := time.NewTicker(checkpointTickerTime)
	go func() {
		for range ticker.C {
			dispatcher.Checkpoint()
		}
	}()
	return ticker
}

func displayStatus(dispatcher *Dispatcher) {
	statusSet := dispatcher.Statuses()

	log.ClearStatus()
	log.StatusPrint(statusSet)
}
