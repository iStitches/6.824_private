package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

// 旋转门状态
type State uint32

const (
	Locked State = iota
	Unlocked
)

// 相关的命令
const (
	CmdCoin = "coin"
	CmdPush = "push"
)

// 状态转换结构体
type CommandStateTupple struct {
	Command string
	State   State
}

// 状态转移函数
type TransitionFunc func(state *State)

type Turnstile struct {
	State State
}

func (p *Turnstile) ExecuteCmd(cmd string) {
	tupple := CommandStateTupple{strings.TrimSpace(cmd), p.State}
	if f := StateTransitionTable[tupple]; f == nil {
		fmt.Println("Unknown command, try again!")
	} else {
		f(&p.State)
	}
}

func main() {
	machine := &Turnstile{
		State: Locked,
	}
	reader := bufio.NewReader(os.Stdin)
	promot(machine.State)
	for {
		// 读取用户输入
		cmd, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		machine.ExecuteCmd(cmd)
	}
}

func promot(s State) {
	m := map[State]string{
		Locked:   "Locked",
		Unlocked: "Unlocked",
	}
	fmt.Printf("当前的状态是： [%s], 请输入命令： [coin|push]\n", m[s])
}

var StateTransitionTable = map[CommandStateTupple]TransitionFunc{
	{CmdCoin, Locked}: func(state *State) {
		fmt.Println("已解锁，请通行")
		*state = Unlocked
	},
	{CmdPush, Locked}: func(state *State) {
		fmt.Println("禁止通行，请先解锁")
	},
	{CmdCoin, Unlocked}: func(state *State) {
		fmt.Println("大兄弟，已解锁了，别浪费钱了")
	},
	{CmdPush, Unlocked}: func(state *State) {
		fmt.Println("请尽快通行，通行后将自动上锁")
		*state = Locked
	},
}
