package wal

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

func FuzzReadRecord(f *testing.F) {
	f.Add(appendFrame(nil, recEntry, putU64s(1, 1, 0)))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 40))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReader(bytes.NewReader(data))
		for i := 0; i < 64; i++ {
			rec, n, err := readRecord(r)
			if err != nil {
				return
			}
			if n <= int64(headerSize) || int64(len(rec.payload)) != n-int64(headerSize)-1 {
				t.Fatal("record size does not match its payload")
			}
		}
	})
}

// Whatever is sitting in the newest segment after a crash, opening the WAL must
// either succeed with something self-consistent or fail cleanly, never panic.
func FuzzOpenAnySegment(f *testing.F) {
	var good []byte
	good = appendFrame(good, recState, putU64s(3, 1))
	good = appendFrame(good, recEntry, append(putU64s(1, 3), byte(raft.EntryNormal), 'x'))
	good = appendFrame(good, recEntry, append(putU64s(2, 3), byte(raft.EntryNoop)))
	f.Add(good)
	f.Add(good[:len(good)-3])
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, segName(1)), data, 0o644); err != nil {
			t.Fatal(err)
		}
		w, err := Open(Options{Dir: dir, NoSync: true})
		if err != nil {
			return
		}
		defer w.Close()
		rec, err := w.Load()
		if err != nil {
			return
		}
		for i, e := range rec.Entries {
			if i > 0 && e.Index != rec.Entries[i-1].Index+1 {
				t.Fatalf("recovered log has a hole: %d then %d", rec.Entries[i-1].Index, e.Index)
			}
		}
		// and it must still accept writes afterwards
		next := rec.SnapIndex + uint64(len(rec.Entries)) + 1
		if err := w.Save(nil, []raft.Entry{{Index: next, Term: rec.HardState.Term + 1}}); err != nil {
			t.Fatalf("recovered WAL refuses writes: %v", err)
		}
	})
}

func FuzzReadSnapFile(f *testing.F) {
	dir := f.TempDir()
	s := raft.Snapshot{Index: 9, Term: 2, Members: []raft.Member{{ID: 1, Addr: "a:1"}, {ID: 2, Addr: "b:2", Learner: true}}, Data: []byte("state")}
	if err := writeSnapFile(dir, s, false); err != nil {
		f.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, snapName(9, 2)))
	f.Add(b)
	f.Add(b[:len(b)/2])
	f.Fuzz(func(t *testing.T, data []byte) {
		p := filepath.Join(t.TempDir(), snapName(1, 1))
		os.WriteFile(p, data, 0o644)
		readSnapFile(p) // an error is fine, a panic is not
	})
}

func FuzzMembersCodec(f *testing.F) {
	f.Add(encodeMembers([]raft.Member{{ID: 7, Addr: "x", Learner: true}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		ms, err := decodeMembers(data)
		if err != nil {
			return
		}
		again, err := decodeMembers(encodeMembers(ms))
		if err != nil || len(again) != len(ms) {
			t.Fatalf("round trip failed: %v", err)
		}
		for i := range ms {
			if again[i] != ms[i] {
				t.Fatalf("member %d changed in the round trip", i)
			}
		}
	})
}

// found by FuzzReadRecord: a header claiming ~800 MB used to allocate that much
// before noticing the file had nothing behind it
func TestHugeLengthDoesNotAllocate(t *testing.T) {
	hdr := []byte("00000000")
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, err := readRecord(bufio.NewReader(bytes.NewReader(hdr)))
	runtime.ReadMemStats(&after)
	if err != errTorn {
		t.Fatalf("want errTorn, got %v", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 8<<20 {
		t.Fatalf("a bogus length allocated %d MiB", grew>>20)
	}
}
