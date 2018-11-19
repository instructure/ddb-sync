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

package operations

import (
	"fmt"
	"sync"
)

type OperationPhase int

const (
	Initialized OperationPhase = iota
	Started
	Finished
	Errored
)

var errBadTransition = fmt.Errorf("PhaseError: unavailable transition")

// Phase a representation of the status of an operational phase
type Phase struct {
	opPhase OperationPhase
	m       sync.RWMutex
}

// StatusCode returns the representative integer of the status
// 0 -> Initialized
// 1 -> Started
// 2 -> Finished
// 3 -> Errored
func (p *Phase) StatusCode() OperationPhase {
	p.m.RLock()
	defer p.m.RUnlock()

	return p.opPhase
}

// Start mark the phase as started
func (p *Phase) Start() error {
	p.transition(Started)
	return nil
}

// Finish mark the phase as finished
func (p *Phase) Finish() error {
	p.transition(Finished)
	return nil
}

// Error mark the phase as errored
func (p *Phase) Error() error {
	_ = p.transition(Errored)
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

func (p *Phase) Running() bool {
	return p.StatusCode() == Started
}

func (p *Phase) Complete() bool {
	return p.StatusCode() == Finished
}

func (p *Phase) Errored() bool {
	return p.StatusCode() == Errored
}

func (p *Phase) transition(toPhase OperationPhase) error {
	p.m.Lock()
	defer p.m.Unlock()

	validTargetPhase := Errored

	switch p.opPhase {
	case Initialized:
		validTargetPhase = Started
	case Started:
		validTargetPhase = Finished
	case Finished:
		return errBadTransition
	case Errored:
		return nil
	}

	if toPhase != validTargetPhase {
		p.opPhase = Errored
		return errBadTransition
	}

	p.opPhase = toPhase
	return nil
}
