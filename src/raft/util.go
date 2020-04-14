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

func RPrintf(term int, id int, format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(fmt.Sprintf("[%03v] [%v] %v", term, id, format), a...)
	}
}
