// Package keyrange holds the [key, end) conventions shared by Range, Delete and Watch.
package keyrange

import "bytes"

// Contains reports whether k falls in the range described by (start, end):
// end empty means exactly start, end == "\x00" means everything >= start.
func Contains(start, end, k []byte) bool {
	if len(end) == 0 {
		return bytes.Equal(start, k)
	}
	if bytes.Compare(k, start) < 0 {
		return false
	}
	return isAll(end) || bytes.Compare(k, end) < 0
}

func isAll(end []byte) bool { return len(end) == 1 && end[0] == 0 }

func IsAll(end []byte) bool { return isAll(end) }

// PrefixEnd returns the smallest key greater than every key with the prefix.
func PrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}
