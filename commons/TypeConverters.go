package commons

func BytesToInt32(b []byte) int32 {
	return int32(b[0]) | int32(b[1])<<8 | int32(b[2])<<16 | int32(b[3])<<24
}

func BytesConcat(bytes ...[]byte) []byte {
	var length int
	for _, b := range bytes {
		length += len(b)
	}
	result := make([]byte, length)
	var index int
	for _, b := range bytes {
		for _, bb := range b {
			result[index] = bb
			index++
		}
	}
	return result
}

func BytesCompare(b1, b2 []byte) bool {
	if len(b1) != len(b2) {
		return false
	}
	for i := range b1 {
		if b1[i] != b2[i] {
			return false
		}
	}
	return true
}
