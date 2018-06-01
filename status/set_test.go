package status_test

import (
	"testing"

	"gerrit.instructure.com/ddb-sync/status"
)

func TestSetDelimiter(t *testing.T) {
	narrowSet := &status.Set{
		ViewportWidth: 20,
	}

	narrowTest := "- Current Status -"
	if narrowSet.Delimiter() != narrowTest {
		t.Errorf("@20 width: set didn't match\nTest   : %q\nPrinted: %q", narrowTest, narrowSet.Delimiter())
	}

	wideSet := &status.Set{
		ViewportWidth: 120,
	}

	wideTest := "--------------------------------------------------- Current Status ---------------------------------------------------"
	if wideSet.Delimiter() != wideTest {
		t.Errorf("@120 width: set didn't match\nTest   : %q\nPrinted: %q", wideTest, wideSet.Delimiter())
	}
}
