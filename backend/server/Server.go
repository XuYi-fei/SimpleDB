package server

import (
	"SimpleDB/backend/tbm"
	"SimpleDB/transport"
	"fmt"
	"io"
	"net"
	"sync"
)

type Server struct {
	port int
	tbm  *tbm.TableManager
}

func NewServer(port int, tbm *tbm.TableManager) *Server {
	return &Server{port: port, tbm: tbm}
}

func (s *Server) Start() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer ln.Close()
	fmt.Println("Server listening on port:", s.port)

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConnection(conn, s.tbm)
		}()
	}
	wg.Wait()
}

func handleConnection(conn net.Conn, tbm *tbm.TableManager) {
	defer conn.Close()
	addr := conn.RemoteAddr().(*net.TCPAddr)
	fmt.Printf("Established connection: %s:%d\n", addr.IP, addr.Port)

	transporter := transport.NewTransporter(conn)
	encoder := &transport.Encoder{}

	packager := transport.NewPackager(transporter, encoder)
	executor := NewExecutor(tbm)

	for {
		pkg, err := packager.Receive()
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error receiving package:", err)
			}
			break
		}

		sql := pkg.Data
		var result []byte
		var execErr error

		result, execErr = executor.Execute(sql)
		pkg = &transport.Package{Data: result, Err: execErr}

		err = packager.Send(pkg)
		if err != nil {
			fmt.Println("Error sending package:", err)
			break
		}
	}

	executor.Close()
	packager.Close()
}
