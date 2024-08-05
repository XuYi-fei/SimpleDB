package transport

import (
	"bufio"
	"encoding/hex"
	"net"
)

// Transporter 结构体，负责处理数据的发送和接收
type Transporter struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// NewTransporter 创建一个新的 Transporter 实例
func NewTransporter(conn net.Conn) *Transporter {
	return &Transporter{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

// Send 发送数据，将数据转换为十六进制字符串后发送
func (t *Transporter) Send(data []byte) error {
	raw := hex.EncodeToString(data) + "\n" // 将数据编码为十六进制并加上换行符
	_, err := t.writer.WriteString(raw)    // 写入缓冲区
	if err != nil {
		return err
	}
	return t.writer.Flush() // 刷新缓冲区，确保数据被发送
}

// Receive 接收数据，读取十六进制字符串并解码
func (t *Transporter) Receive() ([]byte, error) {
	line, err := t.reader.ReadString('\n') // 读取一行数据
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1]              // 去掉行末的换行符
	decoded, err := hex.DecodeString(line) // 解码十六进制字符串
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

// Close 关闭连接
func (t *Transporter) Close() error {
	return t.conn.Close()
}
