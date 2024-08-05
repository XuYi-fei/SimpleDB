package transport

type Packager struct {
	transporter *Transporter
	encoder     *Encoder
}

func NewPackager(transporter *Transporter, encoder *Encoder) *Packager {
	return &Packager{
		transporter: transporter,
		encoder:     encoder,
	}
}

func (packager *Packager) Send(pkg *Package) error {
	data := packager.encoder.Encode(pkg)
	err := packager.transporter.Send(data)
	if err != nil {
		return err
	}
	return nil
}

func (packager *Packager) Receive() (*Package, error) {
	data, err := packager.transporter.Receive()
	if err != nil {
		return nil, err
	}
	pkg, err := packager.encoder.Decode(data)
	if err != nil {
		return nil, err
	}
	return pkg, nil
}

func (packager *Packager) Close() error {
	return packager.transporter.Close()
}
