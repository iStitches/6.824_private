package raft

import (
	"sync"
	"time"
)

type Timer struct {
	waitMS  int
	command SMTransfer // when timer arrive, send command into chan
	raft    *Raft
	timer1  *time.Timer
	mu      sync.Mutex
}

func MakeTimer(waitMS int, command SMTransfer, raft *Raft) *Timer {
	timer := &Timer{
		waitMS:  waitMS,
		command: command,
		raft:    raft,
	}
	timer.timer1 = time.NewTimer(10000 * time.Second)
	// use Closures to stop timer, and wait for using
	go func(timer *Timer) {
		timer.Stop()
		for {
			<-timer.timer1.C
			timer.raft.stateMachine.issueTrans(timer.command)
			timer.Start()
		}
	}(timer)
	return timer
}

//
// stop Timer and wait for next use
// if t.timer1 has stopped, we still have to empty the data in chan, we use select to escape from blocking
//
func (t *Timer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.timer1.Stop() {
		select {
		case <-t.timer1.C:
		default:
		}
	}
}

func (t *Timer) SetWaitMS(waitMS int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.waitMS = waitMS
}

//
// start Timer
// before starting Timer, we need to clean up data in the chan
//
func (t *Timer) Start() {
	t.Stop()
	t.mu.Lock()
	defer t.mu.Unlock()
	duration := time.Duration(t.waitMS) * time.Millisecond
	t.timer1.Reset(duration)
}

func (t *Timer) Clean() {
	t.Start()
}
