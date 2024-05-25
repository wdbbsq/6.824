package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ClearChannel(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
