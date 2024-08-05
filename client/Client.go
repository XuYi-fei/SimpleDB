package client

import (
	"SimpleDB/transport"
)

type Client struct {
	// rt RoundTripper实例，用于处理请求的往返传输
	rt *RoundTripper
}

// NewClient 接收一个Packager对象作为参数，并创建一个新的RoundTripper实例
func NewClient(packager *transport.Packager) *Client {
	rt := NewRoundTripper(packager)
	return &Client{
		rt: rt,
	}
}

// Execute 接收一个字节数组作为参数，将其封装为一个Package对象，并通过RoundTripper发送
// 如果响应的Package对象中包含错误，那么抛出这个错误
// 否则，返回响应的Package对象中的数据
func (client *Client) Execute(stat []byte) ([]byte, error) {
	pkg := transport.NewPackage(stat, nil)
	resPkg, err := client.rt.RoundTrip(pkg)
	if err != nil {
		return nil, err
	}
	return resPkg.GetData(), resPkg.GetErr()
}

func (client *Client) Close() {
	client.rt.Close()
}
