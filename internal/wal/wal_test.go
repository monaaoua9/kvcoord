package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

func ents(term uint64, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("payload-%d", i))})
	}
	return out
}

func open(t *testing.T, dir string, seg int64) *WAL {
	t.Helper()
	w, err := Open(Options{Dir: dir, SegmentSize: seg, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func reopen(t *testing.T, w *WAL, dir string, seg int64) (*WAL, raft.Recovered) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2 := open(t, dir, seg)
	rec, err := w2.Load()
	if err != nil {
		t.Fatal(err)
	}
	return w2, rec
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 0)
	if err := w.Save(&raft.HardState{Term: 3, Vote: 2}, ents(3, 1, 10)); err != nil {
		t.Fatal(err)
	}
	w, rec := reopen(t, w, dir, 0)
	defer w.Close()
	if rec.HardState != (raft.HardState{Term: 3, Vote: 2}) {
		t.Fatalf("hard state %+v", rec.HardState)
	}
	if len(rec.Entries) != 10 || rec.Entries[9].Index != 10 || string(rec.Entries[4].Data) != "payload-5" {
		t.Fatalf("entries %+v", rec.Entries)
	}
}

func TestOverwriteTruncatesSuffix(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 0)
	w.Save(nil, ents(1, 1, 10))
	w.Save(nil, ents(2, 6, 8)) // new leader rewrites 6.. and drops 9, 10
	w, rec := reopen(t, w, dir, 0)
	defer w.Close()
	if len(rec.Entries) != 8 {
		t.Fatalf("want 8 entries, got %d", len(rec.Entries))
	}
	if rec.Entries[4].Term != 1 || rec.Entries[5].Term != 2 || rec.Entries[7].Term != 2 {
		t.Fatalf("terms wrong: %+v", rec.Entries)
	}
}

func TestTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 0)
	w.Save(nil, ents(1, 1, 5))
	path := w.segs[len(w.segs)-1].path
	w.Close()

	st, _ := os.Stat(path)
	if err := os.Truncate(path, st.Size()-5); err != nil {
		t.Fatal(err)
	}
	w = open(t, dir, 0)
	rec, _ := w.Load()
	if len(rec.Entries) != 4 {
		t.Fatalf("want 4 intact entries, got %d", len(rec.Entries))
	}
	// the log must be appendable again after truncating the torn bytes
	if err := w.Save(nil, ents(1, 5, 6)); err != nil {
		t.Fatal(err)
	}
	w, rec = reopen(t, w, dir, 0)
	defer w.Close()
	if len(rec.Entries) != 6 {
		t.Fatalf("want 6 entries after repair, got %d", len(rec.Entries))
	}
}

func TestCorruptionInMiddleSegmentFails(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 200)
	for i := uint64(1); i <= 40; i++ {
		w.Save(nil, ents(1, i, i))
	}
	if w.SegmentCount() < 3 {
		t.Fatalf("expected rotation, have %d segments", w.SegmentCount())
	}
	first := w.segs[0].path
	w.Close()

	b, _ := os.ReadFile(first)
	b[len(b)/2] ^= 0xff
	os.WriteFile(first, b, 0o644)
	if _, err := Open(Options{Dir: dir, SegmentSize: 200, NoSync: true}); err == nil {
		t.Fatal("expected corruption error")
	}
}

func TestSnapshotCompactsSegments(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 300)
	w.Save(&raft.HardState{Term: 2, Vote: 1}, nil)
	for i := uint64(1); i <= 100; i++ {
		w.Save(nil, ents(2, i, i))
	}
	before := w.SegmentCount()
	if before < 4 {
		t.Fatalf("expected several segments, have %d", before)
	}

	if err := w.SaveSnapshot(raft.Snapshot{Index: 80, Term: 2, Data: []byte("state@80")}); err != nil {
		t.Fatal(err)
	}
	if after := w.SegmentCount(); after >= before {
		t.Fatalf("segments not compacted: %d -> %d", before, after)
	}
	w.Save(nil, ents(2, 101, 105))

	w, rec := reopen(t, w, dir, 300)
	defer w.Close()
	if rec.SnapIndex != 80 || rec.SnapTerm != 2 {
		t.Fatalf("snapshot meta %d/%d", rec.SnapIndex, rec.SnapTerm)
	}
	if len(rec.Entries) != 25 || rec.Entries[0].Index != 81 || rec.Entries[24].Index != 105 {
		t.Fatalf("entries after snapshot: %d first=%d", len(rec.Entries), rec.Entries[0].Index)
	}
	if rec.HardState.Term != 2 || rec.HardState.Vote != 1 {
		t.Fatalf("hard state lost across compaction: %+v", rec.HardState)
	}
	s, ok, err := w.LoadSnapshot()
	if err != nil || !ok || string(s.Data) != "state@80" {
		t.Fatalf("snapshot load: %v %v %q", ok, err, s.Data)
	}
}

func TestSnapshotBeyondLogDropsEverything(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 0)
	w.Save(nil, ents(1, 1, 5))
	w.SaveSnapshot(raft.Snapshot{Index: 50, Term: 4, Data: []byte("x")})
	w.Save(nil, ents(4, 51, 52))
	w, rec := reopen(t, w, dir, 0)
	defer w.Close()
	if rec.SnapIndex != 50 || len(rec.Entries) != 2 || rec.Entries[0].Index != 51 {
		t.Fatalf("%+v", rec)
	}
}

func TestOldSnapshotIgnored(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 0)
	defer w.Close()
	w.Save(nil, ents(1, 1, 30))
	w.SaveSnapshot(raft.Snapshot{Index: 20, Term: 1, Data: []byte("new")})
	w.SaveSnapshot(raft.Snapshot{Index: 10, Term: 1, Data: []byte("old")})
	s, _, _ := w.LoadSnapshot()
	if s.Index != 20 {
		t.Fatalf("newest snapshot is %d", s.Index)
	}
}

func TestCorruptNewestSnapshotFallsBack(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 0)
	w.Save(nil, ents(1, 1, 30))
	w.SaveSnapshot(raft.Snapshot{Index: 10, Term: 1, Data: []byte("a")})
	w.Close()
	// a half-written newer snapshot file must not shadow the good one
	bad := filepath.Join(dir, "snap", snapName(20, 1))
	os.WriteFile(bad, []byte("garbage"), 0o644)
	w = open(t, dir, 0)
	defer w.Close()
	s, ok, _ := w.LoadSnapshot()
	if !ok || s.Index != 10 {
		t.Fatalf("got %+v ok=%v", s.Index, ok)
	}
}

// Cutting the file at any byte is what a crash during write(2) looks like.
// Whatever survives must be a clean prefix of what was written.
func TestEveryTruncationPointRecoversAPrefix(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir, 0)
	w.Save(&raft.HardState{Term: 1, Vote: 3}, ents(1, 1, 3))
	w.Save(nil, ents(1, 4, 9))
	w.Save(&raft.HardState{Term: 2, Vote: 0}, ents(2, 7, 10)) // overwrites 7..9
	path := w.segs[len(w.segs)-1].path
	w.Close()

	orig, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	full := open(t, dir, 0)
	want, _ := full.Load()
	full.Close()
	if len(want.Entries) != 10 {
		t.Fatalf("baseline has %d entries", len(want.Entries))
	}

	for cut := 0; cut <= len(orig); cut++ {
		os.WriteFile(path, orig[:cut], 0o644)
		w2, err := Open(Options{Dir: dir, NoSync: true})
		if err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		rec, _ := w2.Load()
		w2.Close()
		for i, e := range rec.Entries {
			if e.Index != uint64(i+1) {
				t.Fatalf("cut at %d: hole at position %d (index %d)", cut, i, e.Index)
			}
		}
		// entries from before the overwrite may legitimately appear with the old term,
		// but any entry that matches the final log must be identical
		for i, e := range rec.Entries {
			if e.Term == want.Entries[i].Term && string(e.Data) != string(want.Entries[i].Data) {
				t.Fatalf("cut at %d: entry %d corrupted", cut, e.Index)
			}
		}
	}
}
