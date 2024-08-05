package main

import (
	"dbofmine/client"
	"dbofmine/transport"
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9998")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	encoder := &transport.Encoder{}
	transporter := transport.NewTransporter(conn)
	packager := transport.NewPackager(transporter, encoder)

	cl := client.NewClient(packager)
	shell := client.NewShell(cl)
	shell.Run()
}
