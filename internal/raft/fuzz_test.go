package raft

import "testing"

func FuzzConfChange(f *testing.F) {
	f.Add(ConfChange{Type: ConfAdd, ID: 3, Addr: "h:1"}.Marshal())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		cc, err := UnmarshalConfChange(data)
		if err != nil {
			return
		}
		again, err := UnmarshalConfChange(cc.Marshal())
		if err != nil || again != cc {
			t.Fatalf("round trip changed %+v into %+v (%v)", cc, again, err)
		}
		// applying any change to any set must keep it sorted and duplicate free
		ms := applyChange([]Member{{ID: 1}, {ID: 5, Learner: true}}, cc)
		for i := 1; i < len(ms); i++ {
			if ms[i-1].ID >= ms[i].ID {
				t.Fatalf("membership not strictly ordered: %v", ms)
			}
		}
	})
}
