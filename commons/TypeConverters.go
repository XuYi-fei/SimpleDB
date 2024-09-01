package commons

import "encoding/binary"

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

// String2Bytes 将字符串转换为字节数组，格式为：[StringLength][StringData]，前者4字节
func String2Bytes(str string) []byte {
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(str)))
	return BytesConcat(length, []byte(str))
}

// Int64ToBytes 将int64转换为字节数组
func Int64ToBytes(i int64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(i))
	return raw
}

type ParseStringResult struct {
	Str  string
	Next int32
}

// ParseString 从字节数组中解析字符串，格式为：[StringLength][StringData]，前者4字节
func ParseString(raw []byte) ParseStringResult {
	length := binary.BigEndian.Uint32(raw[:4])
	str := string(raw[4 : 4+length])
	return ParseStringResult{
		Str:  str,
		Next: 4 + int32(length),
	}
}

// Str2Uid 根据key的字符串，生成一个Uid/Key，这个是用来构建索引的，对于数字直接转换即可，字符串则需要这个函数
func Str2Uid(key string) int64 {
	var seed int64 = 13331
	var result int64 = 0
	for _, c := range key {
		result = result*seed + int64(c)
	}
	return result
}
