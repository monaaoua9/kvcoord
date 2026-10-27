package raft

import (
	"encoding/binary"
	"errors"
	"slices"
)

// Member is a cluster member and the opaque address the transport dials it at.
// A learner receives the log but does not vote, campaign or count toward any quorum.
type Member struct {
	ID      NodeID
	Addr    string
	Learner bool
}

type ConfChangeType uint8

const (
	ConfAdd ConfChangeType = iota + 1
	ConfRemove
	ConfAddLearner
	ConfPromote // learner becomes a voter
)

// ConfChange adds or removes exactly one voter. Changing one at a time keeps
// every old majority and every new majority overlapping, which is why no joint
// consensus is needed.
type ConfChange struct {
	Type ConfChangeType
	ID   NodeID
	Addr string
}

func (c ConfChange) Marshal() []byte {
	b := make([]byte, 0, 9+len(c.Addr))
	b = append(b, byte(c.Type))
	b = binary.BigEndian.AppendUint64(b, c.ID)
	return append(b, c.Addr...)
}

func UnmarshalConfChange(b []byte) (ConfChange, error) {
	if len(b) < 9 {
		return ConfChange{}, errors.New("raft: short conf change")
	}
	return ConfChange{
		Type: ConfChangeType(b[0]),
		ID:   binary.BigEndian.Uint64(b[1:9]),
		Addr: string(b[9:]),
	}, nil
}

// applyChange returns a new sorted slice, ms is left alone.
func applyChange(ms []Member, cc ConfChange) []Member {
	out := make([]Member, 0, len(ms)+1)
	for _, m := range ms {
		if m.ID != cc.ID {
			out = append(out, m)
		}
	}
	switch cc.Type {
	case ConfAdd:
		out = append(out, Member{ID: cc.ID, Addr: cc.Addr})
	case ConfAddLearner:
		out = append(out, Member{ID: cc.ID, Addr: cc.Addr, Learner: true})
	case ConfPromote:
		for _, m := range ms {
			if m.ID == cc.ID {
				m.Learner = false
				out = append(out, m)
			}
		}
	}
	slices.SortFunc(out, func(a, b Member) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	return out
}

func memberIDs(ms []Member) []NodeID {
	ids := make([]NodeID, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	return ids
}

func voterIDs(ms []Member) []NodeID {
	var ids []NodeID
	for _, m := range ms {
		if !m.Learner {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

func isLearner(ms []Member, id NodeID) bool {
	return slices.ContainsFunc(ms, func(m Member) bool { return m.ID == id && m.Learner })
}

func hasMember(ms []Member, id NodeID) bool {
	return slices.ContainsFunc(ms, func(m Member) bool { return m.ID == id })
}
