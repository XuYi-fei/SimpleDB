package client

import "SimpleDB/transport"

// RoundTripper 用于发送请求并接受响应
type RoundTripper struct {
	packager *transport.Packager
}

func NewRoundTripper(packager *transport.Packager) *RoundTripper {
	return &RoundTripper{
		packager: packager,
	}
}

// RoundTrip 用于处理请求的往返传输
func (roundTripper *RoundTripper) RoundTrip(pkg *transport.Package) (*transport.Package, error) {
	// 发送请求包
	err := roundTripper.packager.Send(pkg)
	if err != nil {
		return nil, err
	}
	// 接收响应包，并返回
	return roundTripper.packager.Receive()
}

func (roundTripper *RoundTripper) Close() error {
	return roundTripper.packager.Close()
}
