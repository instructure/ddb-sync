package operations_test

import (
	"testing"

	"gerrit.instructure.com/ddb-sync/operations"
)

func TestPhaseTransitions(t *testing.T) {
	opPhase := operations.Phase{}

	err := opPhase.Start()
	if err != nil {
		t.Errorf("'Initialized' state should safely transition on 'Start'")
	}

	err = opPhase.Finish()
	if err != nil {
		t.Errorf("'Started' state should safely transition on 'Finish'")
	}

}
