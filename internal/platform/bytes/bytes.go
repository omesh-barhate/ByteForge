package bytes

// TrimZeroBytes returns a byte slice without the ending zero bytes
// For example, if [data] is a 4byte fix-sized slice like this:
// "id\x00\x00"
// The function returns "id"
func TrimZeroBytes(data []byte) []byte {
	n := 0
	for {
		if n >= len(data) {
			break
		}
		if data[n] == 0 {
			break
		}
		n++
	}
	res := make([]byte, n)
	copy(res, data[:n])
	return res
}
