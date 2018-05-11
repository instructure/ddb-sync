package main

import (
	"fmt"
	"sync"
)

type operationPhase int

const (
	Initialized operationPhase = iota
	Started
	Finished
	Errored
)

var badTransition = fmt.Errorf("PhaseError: unavailable transition")

// Phase a representation of the status of an operational phase
type Phase struct {
	opPhase operationPhase
	m       sync.RWMutex
}

func (p *Phase) update(phase operationPhase) {
	p.m.Lock()
	defer p.m.Unlock()

	p.opPhase = phase
}

// StatusCode returns the representative integer of the status
// 0 -> Initialized
// 1 -> Started
// 2 -> Finished
// 3 -> Errored
func (p *Phase) StatusCode() operationPhase {
	p.m.RLock()
	defer p.m.RUnlock()

	return p.opPhase
}

// Start mark the phase as started
func (p *Phase) Start() error {
	if p.StatusCode() != Initialized {
		p.update(Errored)
		return badTransition
	}
	p.update(Started)
	return nil
}

// Finish mark the phase as finished
func (p *Phase) Finish() error {
	if p.StatusCode() != Started {
		p.update(Errored)
		return badTransition
	}
	p.update(Finished)
	return nil
}

// Error mark the phase as errored
func (p *Phase) Error() error {
	ogPhase := p.StatusCode()
	p.update(Errored)
	if ogPhase == Errored {
		return badTransition
	}
	return nil
}

// Status return the string representation of the phase status
func (p *Phase) Status() string {
	switch p.StatusCode() {
	case Initialized:
		return "Initialized"
	case Started:
		return "Started"
	case Finished:
		return "Finished"
	case Errored:
		return "Errored"
	}
	return ""
}
