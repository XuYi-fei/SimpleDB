package client

import (
	"bufio"
	"fmt"
	"os"
)

// Shell 用于接受用户的输入，并调用Client.execute()
type Shell struct {
	client *Client
}

func NewShell(client *Client) *Shell {
	return &Shell{
		client: client,
	}
}

func (shell *Shell) Run() {
	// 用于读取用户的输入
	scanner := bufio.NewScanner(os.Stdin)
	defer shell.client.Close()

	for {
		fmt.Print(":> ")
		if !scanner.Scan() {
			break
		}
		statStr := scanner.Text()
		if statStr == "exit" || statStr == "quit" {
			break
		}
		res, err := shell.client.Execute([]byte(statStr))
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println(string(res))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading from input:", err)
	}
}
