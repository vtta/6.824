package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func RPrintf(term int, id int, state RaftState, format string, a ...interface{}) {
	if Debug > 0 {
		format = fmt.Sprintf("[%03v] [%v] [%v] %v", term, id, state, format)
		log.Printf(format, a...)
	}
}
