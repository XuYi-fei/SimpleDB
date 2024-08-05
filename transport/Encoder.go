package transport

import (
	"SimpleDB/commons"
	"errors"
)

type Encoder struct {
}

func (e *Encoder) Encode(pkg *Package) []byte {
	if pkg.GetErr() != nil {
		err := pkg.GetErr()
		msg := err.Error()
		if msg == "" {
			msg = "Internal server error!"
		}
		return commons.BytesConcat([]byte{1}, []byte(msg))
	} else {
		return commons.BytesConcat([]byte{0}, pkg.GetData())
	}
}

func (e *Encoder) Decode(data []byte) (*Package, error) {
	if len(data) < 1 {
		return nil, errors.New(commons.ErrorMessage.InvalidPkgDataError)
	}

	if data[0] == 0 {
		return &Package{Data: data[1:]}, nil
	} else if data[0] == 1 {
		return &Package{Err: errors.New(string(data[1:]))}, nil
	} else {
		return nil, errors.New(commons.ErrorMessage.InvalidPkgDataError)
	}
}
