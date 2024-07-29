package utils

func GenerateUID(pageNumber int, offset int) int64 {
	u0 := int64(pageNumber)
	u1 := int64(offset)
	return (u0 << 32) | u1
}
