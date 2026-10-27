// Package wal is the durable side of raft: a segmented, checksummed
// write-ahead log plus snapshot files. It implements raft.Storage.
package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/monaaoua9/kvcoord/internal/raft"
)

type Options struct {
	Dir         string
	SegmentSize int64 // bytes before rotating; default 64 MiB
	NoSync      bool  // tests only: skip fsync
}

type segment struct {
	seq      uint64
	path     string
	maxIndex uint64
}

type WAL struct {
	opts    Options
	walDir  string
	snapDir string

	mu       sync.Mutex
	segs     []*segment // last one is the active segment
	f        *os.File
	size     int64
	hs       raft.HardState
	snapIdx  uint64
	snapTerm uint64
	buf      []byte
	closed   bool

	rec    raft.Recovered
	syncs  atomic.Uint64
	nbytes atomic.Uint64
}

var _ raft.Storage = (*WAL)(nil)

func Open(opts Options) (*WAL, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 64 << 20
	}
	w := &WAL{
		opts:    opts,
		walDir:  filepath.Join(opts.Dir, "wal"),
		snapDir: filepath.Join(opts.Dir, "snap"),
	}
	for _, d := range []string{w.walDir, w.snapDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := w.recover(); err != nil {
		return nil, err
	}
	return w, nil
}

func segName(seq uint64) string { return fmt.Sprintf("%016x.wal", seq) }

func (w *WAL) listSegments() ([]*segment, error) {
	ents, err := os.ReadDir(w.walDir)
	if err != nil {
		return nil, err
	}
	var segs []*segment
	for _, e := range ents {
		var seq uint64
		if !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		if _, err := fmt.Sscanf(strings.TrimSuffix(e.Name(), ".wal"), "%016x", &seq); err != nil {
			continue
		}
		segs = append(segs, &segment{seq: seq, path: filepath.Join(w.walDir, e.Name())})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	return segs, nil
}

func (w *WAL) recover() error {
	segs, err := w.listSegments()
	if err != nil {
		return err
	}

	var (
		hs               raft.HardState
		markIdx, markTrm uint64
		all              []raft.Entry
	)
	for i, s := range segs {
		last := i == len(segs)-1
		good, err := scanSegment(s, func(r record) error {
			switch r.typ {
			case recState:
				hs = raft.HardState{Term: getU64(r.payload, 0), Vote: getU64(r.payload, 1)}
			case recSnapMark:
				if idx := getU64(r.payload, 0); idx >= markIdx {
					markIdx, markTrm = idx, getU64(r.payload, 1)
				}
			case recEntry:
				e := raft.Entry{
					Index: getU64(r.payload, 0),
					Term:  getU64(r.payload, 1),
					Type:  raft.EntryType(r.payload[16]),
				}
				if len(r.payload) > 17 {
					e.Data = append([]byte(nil), r.payload[17:]...)
				}
				if e.Index > s.maxIndex {
					s.maxIndex = e.Index
				}
				all = append(all, e)
			default:
				return errBadType
			}
			return nil
		})
		if err == errTorn && last {
			// a crash in the middle of an append; everything before it is good
			if terr := os.Truncate(s.path, good); terr != nil {
				return terr
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("wal: segment %s: %w", filepath.Base(s.path), err)
		}
	}

	base, baseTerm := markIdx, markTrm
	snap, haveSnap, err := newestSnapshot(w.snapDir)
	if err != nil {
		return err
	}
	switch {
	case haveSnap && snap.Index >= markIdx:
		base, baseTerm = snap.Index, snap.Term
	case markIdx > 0:
		return fmt.Errorf("wal: log is compacted up to %d but no matching snapshot file exists", markIdx)
	}

	var log []raft.Entry
	for _, e := range all {
		if e.Index <= base {
			continue
		}
		next := base + uint64(len(log)) + 1
		if e.Index > next {
			return fmt.Errorf("wal: gap in log: have up to %d, next record is %d", next-1, e.Index)
		}
		log = append(log[:e.Index-base-1], e)
	}

	w.hs, w.snapIdx, w.snapTerm = hs, base, baseTerm
	w.rec = raft.Recovered{HardState: hs, SnapIndex: base, SnapTerm: baseTerm, Entries: log}
	w.segs = segs

	if len(segs) == 0 {
		return w.newSegment(1)
	}
	cur := segs[len(segs)-1]
	f, err := os.OpenFile(cur.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.f, w.size = f, st.Size()
	return nil
}

// scanSegment feeds every valid record to fn. On a torn tail it returns
// errTorn together with the offset of the last good byte.
func scanSegment(s *segment, fn func(record) error) (int64, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 64<<10)
	var off int64
	for {
		r, n, err := readRecord(br)
		if err == io.EOF {
			return off, nil
		}
		if err != nil {
			return off, err
		}
		if err := fn(r); err != nil {
			return off, err
		}
		off += n
	}
}

func (w *WAL) newSegment(seq uint64) error {
	path := filepath.Join(w.walDir, segName(seq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	w.f, w.size = f, 0
	w.segs = append(w.segs, &segment{seq: seq, path: path})
	if !w.opts.NoSync {
		syncDir(w.walDir)
	}
	// every segment starts with the current state so older ones can be dropped
	buf := appendFrame(nil, recState, putU64s(w.hs.Term, w.hs.Vote))
	buf = appendFrame(buf, recSnapMark, putU64s(w.snapIdx, w.snapTerm))
	return w.write(buf)
}

func (w *WAL) write(b []byte) error {
	n, err := w.f.Write(b)
	w.size += int64(n)
	w.nbytes.Add(uint64(n))
	return err
}

func (w *WAL) sync() error {
	if w.opts.NoSync {
		return nil
	}
	w.syncs.Add(1)
	return w.f.Sync()
}

func (w *WAL) Load() (raft.Recovered, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// handed out once; raft keeps its own copy and ours would just pin memory
	rec := w.rec
	w.rec = raft.Recovered{HardState: rec.HardState, SnapIndex: rec.SnapIndex, SnapTerm: rec.SnapTerm}
	return rec, nil
}

func (w *WAL) Save(hs *raft.HardState, ents []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("wal: closed")
	}

	buf := w.buf[:0]
	if hs != nil {
		buf = appendFrame(buf, recState, putU64s(hs.Term, hs.Vote))
	}
	var scratch []byte
	cur := w.segs[len(w.segs)-1]
	for _, e := range ents {
		scratch = append(scratch[:0], putU64s(e.Index, e.Term)...)
		scratch = append(scratch, byte(e.Type))
		scratch = append(scratch, e.Data...)
		buf = appendFrame(buf, recEntry, scratch)
		if e.Index > cur.maxIndex {
			cur.maxIndex = e.Index
		}
	}
	w.buf = buf[:0]
	if len(buf) == 0 {
		return nil
	}
	if err := w.write(buf); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}
	if hs != nil {
		w.hs = *hs
	}
	if w.size >= w.opts.SegmentSize {
		return w.rotate()
	}
	return nil
}

func (w *WAL) rotate() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.newSegment(w.segs[len(w.segs)-1].seq + 1)
}

func (w *WAL) SaveSnapshot(s raft.Snapshot) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("wal: closed")
	}
	if s.Index <= w.snapIdx {
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()

	// the file is written outside the lock, it can be large
	if err := writeSnapFile(w.snapDir, s, !w.opts.NoSync); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if s.Index <= w.snapIdx {
		return nil
	}
	if err := w.write(appendFrame(nil, recSnapMark, putU64s(s.Index, s.Term))); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}
	w.snapIdx, w.snapTerm = s.Index, s.Term

	// drop a prefix of segments that hold nothing newer than the snapshot
	drop := 0
	for drop < len(w.segs)-1 && w.segs[drop].maxIndex <= s.Index {
		drop++
	}
	for _, seg := range w.segs[:drop] {
		os.Remove(seg.path)
	}
	w.segs = w.segs[drop:]
	pruneSnapshots(w.snapDir, 2)
	return nil
}

func (w *WAL) LoadSnapshot() (raft.Snapshot, bool, error) {
	return newestSnapshot(w.snapDir)
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	err := w.sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (w *WAL) SyncCount() uint64 { return w.syncs.Load() }

func (w *WAL) BytesWritten() uint64 { return w.nbytes.Load() }

func (w *WAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segs)
}
