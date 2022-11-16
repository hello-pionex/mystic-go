package topicwriter

import (
	"time"
)

func WaitFor(fn func() bool, maxWait time.Duration, interval time.Duration) bool {
	deadline := time.Now().Add(maxWait)
	for {
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}

		time.Sleep(interval)
	}
}
