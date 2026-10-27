package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

const snapMagic = 0x4b565333 // "KVS3"

func snapName(index, term uint64) string {
	return fmt.Sprintf("%016x-%016x.snap", index, term)
}

func parseSnapName(name string) (index, term uint64, ok bool) {
	if !strings.HasSuffix(name, ".snap") {
		return
	}
	_, err := fmt.Sscanf(strings.TrimSuffix(name, ".snap"), "%016x-%016x", &index, &term)
	return index, term, err == nil
}

func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return // directories cannot be opened for sync there
	}
	if f, err := os.Open(dir); err == nil {
		f.Sync()
		f.Close()
	}
}

func writeSnapFile(dir string, s raft.Snapshot, sync bool) error {
	final := filepath.Join(dir, snapName(s.Index, s.Term))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	members := encodeMembers(s.Members)
	hdr := make([]byte, 4+8+8+8+8+4)
	binary.LittleEndian.PutUint32(hdr[0:], snapMagic)
	binary.LittleEndian.PutUint64(hdr[4:], s.Index)
	binary.LittleEndian.PutUint64(hdr[12:], s.Term)
	binary.LittleEndian.PutUint64(hdr[20:], uint64(len(members)))
	binary.LittleEndian.PutUint64(hdr[28:], uint64(len(s.Data)))
	crc := crc32.Update(crc32.Checksum(members, crcTable), crcTable, s.Data)
	binary.LittleEndian.PutUint32(hdr[36:], crc)
	_, err = f.Write(hdr)
	if err == nil {
		_, err = f.Write(members)
	}
	if err == nil {
		_, err = f.Write(s.Data)
	}
	if err == nil && sync {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if sync {
		syncDir(dir)
	}
	return nil
}

func readSnapFile(path string) (raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return raft.Snapshot{}, err
	}
	const hdrLen = 40
	if len(b) < hdrLen || binary.LittleEndian.Uint32(b) != snapMagic {
		return raft.Snapshot{}, fmt.Errorf("wal: %s: bad snapshot header", path)
	}
	s := raft.Snapshot{
		Index: binary.LittleEndian.Uint64(b[4:]),
		Term:  binary.LittleEndian.Uint64(b[12:]),
	}
	mlen := binary.LittleEndian.Uint64(b[20:])
	dlen := binary.LittleEndian.Uint64(b[28:])
	if uint64(len(b)-hdrLen) != mlen+dlen {
		return raft.Snapshot{}, fmt.Errorf("wal: %s: truncated snapshot", path)
	}
	body := b[hdrLen:]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(b[36:]) {
		return raft.Snapshot{}, fmt.Errorf("wal: %s: snapshot checksum mismatch", path)
	}
	ms, err := decodeMembers(body[:mlen])
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("wal: %s: %w", path, err)
	}
	s.Members, s.Data = ms, body[mlen:]
	return s, nil
}

// newestSnapshot walks snapshot files from newest to oldest and returns the
// first one that passes its checksum.
func newestSnapshot(dir string) (raft.Snapshot, bool, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return raft.Snapshot{}, false, err
	}
	var names []string
	for _, e := range ents {
		if _, _, ok := parseSnapName(e.Name()); ok {
			names = append(names, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for _, n := range names {
		if s, err := readSnapFile(filepath.Join(dir, n)); err == nil {
			return s, true, nil
		}
	}
	return raft.Snapshot{}, false, nil
}

func pruneSnapshots(dir string, keep int) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var names []string
	for _, e := range ents {
		n := e.Name()
		if _, _, ok := parseSnapName(n); ok {
			names = append(names, n)
		} else if strings.HasSuffix(n, ".tmp") {
			os.Remove(filepath.Join(dir, n))
		}
	}
	sort.Strings(names)
	for len(names) > keep {
		os.Remove(filepath.Join(dir, names[0]))
		names = names[1:]
	}
}

func encodeMembers(ms []raft.Member) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(ms)))
	for _, m := range ms {
		b = binary.LittleEndian.AppendUint64(b, m.ID)
		var flags byte
		if m.Learner {
			flags = 1
		}
		b = append(b, flags)
		b = binary.LittleEndian.AppendUint16(b, uint16(len(m.Addr)))
		b = append(b, m.Addr...)
	}
	return b
}

func decodeMembers(b []byte) ([]raft.Member, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("short member block")
	}
	n := binary.LittleEndian.Uint32(b)
	b = b[4:]
	var out []raft.Member
	for i := uint32(0); i < n; i++ {
		if len(b) < 11 {
			return nil, fmt.Errorf("short member block")
		}
		id := binary.LittleEndian.Uint64(b)
		learner := b[8]&1 != 0
		l := int(binary.LittleEndian.Uint16(b[9:]))
		if len(b) < 11+l {
			return nil, fmt.Errorf("short member block")
		}
		out = append(out, raft.Member{ID: id, Addr: string(b[11 : 11+l]), Learner: learner})
		b = b[11+l:]
	}
	return out, nil
}
