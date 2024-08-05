package transport

type Package struct {
	Data []byte
	Err  error
}

func NewPackage(data []byte, err error) *Package {
	return &Package{
		Data: data,
		Err:  err,
	}
}

func (pack *Package) GetData() []byte {
	return pack.Data
}

func (pack *Package) GetErr() error {
	return pack.Err
}
