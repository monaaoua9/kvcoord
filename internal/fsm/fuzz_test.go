package fsm

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/raft"
)

func seedCommands(f *testing.F) {
	for _, c := range []*kvpb.Command{
		{Body: &kvpb.Command_OpenSession{OpenSession: &kvpb.OpenSessionRequest{}}},
		{Body: &kvpb.Command_Put{Put: &kvpb.PutRequest{Meta: &kvpb.RequestMeta{ClientId: 1, Seq: 1}, Key: []byte("a"), Value: []byte("1")}}},
		{Body: &kvpb.Command_Txn{Txn: &kvpb.TxnRequest{Meta: &kvpb.RequestMeta{ClientId: 1, Seq: 2},
			Compare: []*kvpb.Compare{{Key: []byte("a"), Target: kvpb.Compare_VALUE}},
			Success: []*kvpb.RequestOp{{Request: &kvpb.RequestOp_Put{Put: &kvpb.PutRequest{Key: []byte("b")}}}}}}},
		{Body: &kvpb.Command_Lock{Lock: &kvpb.LockRequest{Name: []byte("l"), Lease: 1}}},
		{Body: &kvpb.Command_Compact{Compact: &kvpb.CompactRequest{Revision: 1}}},
		{Body: &kvpb.Command_LeaseExpire{LeaseExpire: &kvpb.LeaseExpireRequest{Ids: []uint64{1, 2}}}},
	} {
		b, _ := proto.Marshal(c)
		f.Add(b)
	}
}

// Log entries are only ever written by our own leader, but a bug or a corrupt
// disk must not turn into a panic that takes every replica down at once.
func FuzzApplyNeverPanics(f *testing.F) {
	seedCommands(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		m := New(Options{HistoryLimit: 8})
		for i := 0; i < 3; i++ {
			m.Apply(raft.Entry{Index: uint64(i + 1), Term: 1, Data: data})
		}
		if _, _, err := m.Range(&kvpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}}); err != nil {
			t.Fatal(err)
		}
	})
}

// Any command stream, however odd, must leave a machine whose snapshot restores
// to a machine with a byte-identical snapshot.
func FuzzSnapshotRestoreExact(f *testing.F) {
	seedCommands(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		a := New(Options{})
		// treat the input as a sequence of commands: split it into 1..4 chunks
		parts := bytes.SplitN(data, []byte{0xff, 0xff}, 4)
		var idx uint64
		for _, p := range parts {
			idx++
			a.Apply(raft.Entry{Index: idx, Term: 1, Data: p})
		}
		s1, err := a.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		b := New(Options{})
		if err := b.Restore(s1); err != nil {
			t.Fatal(err)
		}
		s2, _ := b.Snapshot()
		if !bytes.Equal(s1, s2) {
			t.Fatal("restore changed the state")
		}
	})
}
