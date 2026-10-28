package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"slices"
)

const (
	recState    byte = 1
	recEntry    byte = 2
	recSnapMark byte = 3

	headerSize = 8
	maxRecord  = 1 << 30
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	errTorn    = errors.New("wal: torn or corrupt record")
	errBadType = errors.New("wal: unknown record type")
)

// frame layout: crc32c(4) | length(4) | type(1) | payload
// length and crc cover type+payload.
func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, typ)
	dst = append(dst, payload...)
	body := dst[start+headerSize:]
	binary.LittleEndian.PutUint32(dst[start:], crc32.Checksum(body, crcTable))
	binary.LittleEndian.PutUint32(dst[start+4:], uint32(len(body)))
	return dst
}

type record struct {
	typ     byte
	payload []byte
}

// readRecord returns errTorn for a short read or checksum mismatch and io.EOF
// only on a clean end of file.
func readRecord(r *bufio.Reader) (record, int64, error) {
	var hdr [headerSize]byte
	if n, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF && n == 0 {
			return record{}, 0, io.EOF
		}
		return record{}, 0, errTorn
	}
	crc := binary.LittleEndian.Uint32(hdr[0:])
	length := binary.LittleEndian.Uint32(hdr[4:])
	if length == 0 || length > maxRecord {
		return record{}, 0, errTorn
	}
	body, err := readBody(r, int(length))
	if err != nil {
		return record{}, 0, errTorn
	}
	if crc32.Checksum(body, crcTable) != crc {
		return record{}, 0, errTorn
	}
	return record{typ: body[0], payload: body[1:]}, int64(headerSize + length), nil
}

// readBody reads n bytes but only allocates as data really arrives: the length
// came from a header that may be garbage, and trusting it would let a damaged
// file demand gigabytes during recovery.
func readBody(r io.Reader, n int) ([]byte, error) {
	const chunk = 1 << 20
	buf := make([]byte, 0, min(n, chunk))
	for len(buf) < n {
		step := min(n-len(buf), chunk)
		buf = slices.Grow(buf, step)
		m, err := io.ReadFull(r, buf[len(buf):len(buf)+step])
		buf = buf[:len(buf)+m]
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func putU64s(vals ...uint64) []byte {
	b := make([]byte, 8*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], v)
	}
	return b
}

func getU64(b []byte, i int) uint64 { return binary.LittleEndian.Uint64(b[i*8:]) }
