package log_test

import (
	"fmt"
	"testing"

	"github.com/instructure/ddb-sync/log"
)

func TestApproximate(t *testing.T) {
	testCase := func(num int, expectation string, t *testing.T) {
		result := log.Approximate(num)
		if result != expectation {
			t.Error(fmt.Errorf("failed: returned val %s != %s when given %d", result, expectation, num))
		}
	}

	testCase(1000, "~1k", t)
	testCase(1000000, "~1m", t)
	testCase(1000000000, "~1b", t)
	testCase(1000000000000, "~1t", t)
	testCase(1499, "~1k", t)
	testCase(1501, "~2k", t)
	testCase(6700, "~7k", t)
	testCase(10000, "~10k", t)
	testCase(100000, "~100k", t)
	testCase(12, "12", t)
	testCase(0, "0", t)
}
