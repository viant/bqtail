package base

import "hash/fnv"

//Hash returns fnv hash value
func Hash(key string) int {
	h :=fnv.New64()
	_, _ = h.Write([]byte(key))
	data := h.Sum(nil)
	keyNumeric := int64(0)
	shift := 0
	for i := 0; i < 8 && i < len(data); i++ {
		v := int64(data[len(data)-1-i])
		if shift == 0 {
			keyNumeric |= v
		} else {
			keyNumeric |= v << uint64(shift)
		}
		shift += 8
	}
	if keyNumeric < 0 {
		keyNumeric *= -1
	}
	return int(keyNumeric)
}

