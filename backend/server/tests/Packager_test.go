package tests

import (
	"SimpleDB/transport"
	"net"
	"testing"
)

func TestPackager(t *testing.T) {
	t.Log("TestPackager")
	serverReady := make(chan struct{})

	go func() {
		ln, err := net.Listen("tcp", ":10345")
		defer ln.Close()
		if err != nil {
			panic(err)
		}
		t.Log("Server started, listening on :10345")
		serverReady <- struct{}{} // 通知服务器已经启动
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		transporter := transport.NewTransporter(conn)
		encoder := &transport.Encoder{}
		p := transport.NewPackager(transporter, encoder)
		one, err := p.Receive()
		if err != nil {
			t.Error(err)
		}
		t.Logf("Received data: %s", string(one.GetData()))
		if string(one.GetData()) != "pkg1 test" {
			t.Errorf("Received data is not correct, got: %s", string(one.GetData()))
		}
		two, err := p.Receive()
		if err != nil {
			t.Error(err)
		}
		t.Logf("Received data: %s", string(two.GetData()))
		if string(two.GetData()) != "pkg2 test" {
			t.Errorf("Received data is not correct, got: %s", string(two.GetData()))
		}
		t.Logf("Sending data: %s", "pkg3 test")
		p.Send(transport.NewPackage([]byte("pkg3 test"), nil))
	}()

	// 等待服务器启动完成
	<-serverReady

	client, err := net.Dial("tcp", "127.0.0.1:10345")
	if err != nil {
		t.Error(err)
	}
	transporter := transport.NewTransporter(client)
	encoder := &transport.Encoder{}
	p := transport.NewPackager(transporter, encoder)
	t.Logf("Sending data: %s", "pkg1 test")
	err = p.Send(transport.NewPackage([]byte("pkg1 test"), nil))
	if err != nil {
		t.Error(err)
	}
	t.Logf("Sending data: %s", "pkg2 test")
	err = p.Send(transport.NewPackage([]byte("pkg2 test"), nil))
	if err != nil {
		t.Error(err)
	}
	three, err := p.Receive()
	if err != nil {
		t.Error(err)
	}
	t.Logf("Received data: %s", string(three.GetData()))
	if string(three.GetData()) != "pkg3 test" {
		t.Errorf("Received data is not correct, got: %s", string(three.GetData()))
	}
}
