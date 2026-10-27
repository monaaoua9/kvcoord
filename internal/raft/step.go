package raft

import (
	"fmt"
	"slices"
)

func (n *Node) becomeFollower(term uint64, lead NodeID) {
	if n.state == Leader {
		n.leaderCleanup()
	}
	n.reset(term)
	n.lead = lead
	n.state = Follower
}

func (n *Node) becomePreCandidate() {
	n.state = PreCandidate
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.lead = 0
}

func (n *Node) becomeCandidate() {
	n.reset(n.term + 1)
	n.vote = n.cfg.ID
	n.hsDirty = true
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.state = Candidate
}

func (n *Node) becomeLeader() {
	n.reset(n.term)
	n.state = Leader
	n.lead = n.cfg.ID
	n.log.Info("became leader", "term", n.term)

	last := n.rlog.lastIndex()
	n.prs = make(map[NodeID]*progress, len(n.peers))
	for _, id := range n.peers {
		n.prs[id] = &progress{next: last + 1}
	}
	// a leader may only commit entries of its own term, so it opens with a no-op
	n.rlog.append(Entry{Index: last + 1, Term: n.term, Type: EntryNoop})
	n.leaderNoop = last + 1
	n.needBcast = true
}

func (n *Node) reset(term uint64) {
	if n.term != term {
		n.term = term
		n.vote = 0
		n.hsDirty = true
	}
	n.lead = 0
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
	n.transferee = 0
	n.votes = nil
	n.prs = nil
	n.resetTimeout()
}

func (n *Node) leaderCleanup() {
	n.waiters.failAll(ErrLeadershipLost)
	nl := &NotLeaderError{}
	for _, r := range n.pendingReads {
		r.req.resp <- readResp{err: nl}
	}
	for _, r := range n.waitReady {
		r.resp <- readResp{err: nl}
	}
	n.pendingReads = nil
	n.waitReady = nil
	n.needBcast = false
}

// campaignTransfer skips PreVote and marks the requests so voters ignore the
// leader lease: the leader itself asked for this election.
func (n *Node) campaignTransfer() {
	n.becomeCandidate()
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
		return
	}
	for _, id := range n.voters {
		if id != n.cfg.ID {
			n.send(Message{Type: MsgVote, To: id, LogIndex: n.rlog.lastIndex(), LogTerm: n.rlog.lastTerm(), Transfer: true})
		}
	}
}

func (n *Node) handleTransfer(t transferReq) {
	switch {
	case n.state != Leader:
		t.resp <- &NotLeaderError{Leader: n.lead}
	case t.target == n.cfg.ID || !n.isVoter(t.target):
		t.resp <- ErrTransferInvalid
	default:
		n.transferee, n.transferElapsed = t.target, 0
		n.log.Info("transferring leadership", "target", t.target)
		if !n.maybeTimeoutNow(t.target) {
			n.bcastTo(t.target, true)
		}
		t.resp <- nil
	}
}

// maybeTimeoutNow fires once the transfer target has the whole log.
func (n *Node) maybeTimeoutNow(id NodeID) bool {
	p := n.prs[id]
	if n.transferee != id || p == nil || p.match < n.rlog.lastIndex() {
		return false
	}
	n.send(Message{Type: MsgTimeoutNow, To: id})
	return true
}

func (n *Node) campaign(pre bool) {
	var (
		typ  MsgType
		term uint64
	)
	if pre {
		n.becomePreCandidate()
		typ, term = MsgPreVote, n.term+1
	} else {
		n.becomeCandidate()
		typ, term = MsgVote, n.term
		if len(n.votes) >= n.quorum() {
			n.becomeLeader()
			return
		}
	}
	n.log.Debug("campaigning", "term", term, "pre", pre)
	for _, id := range n.voters {
		if id == n.cfg.ID {
			continue
		}
		n.send(Message{Type: typ, Term: term, To: id, LogIndex: n.rlog.lastIndex(), LogTerm: n.rlog.lastTerm()})
	}
}

func (n *Node) poll(id NodeID, granted bool) (yes, no int) {
	if _, seen := n.votes[id]; !seen {
		n.votes[id] = granted
	}
	for _, g := range n.votes {
		if g {
			yes++
		} else {
			no++
		}
	}
	return
}

func (n *Node) step(m Message) {
	if !slices.Contains(n.peers, m.From) {
		// a node that is not yet (or no longer) a voter still has to accept a
		// leader's log, otherwise it could never be added
		joining := !hasMember(n.members, n.cfg.ID)
		if !joining || (m.Type != MsgApp && m.Type != MsgHeartbeat && m.Type != MsgSnap) {
			return
		}
	}

	switch {
	case m.Term > n.term:
		isVote := m.Type == MsgVote || m.Type == MsgPreVote
		if isVote && !m.Transfer && !n.cfg.DisableCheckQuorum && n.lead != 0 && n.electionElapsed < n.cfg.ElectionTick {
			// we heard from a leader recently: this is a disruptive node, ignore it
			return
		}
		switch m.Type {
		case MsgPreVote:
		case MsgPreVoteResp:
			if m.Reject {
				n.becomeFollower(m.Term, 0)
			}
		case MsgApp, MsgHeartbeat, MsgSnap:
			n.becomeFollower(m.Term, m.From)
		default:
			n.becomeFollower(m.Term, 0)
		}
	case m.Term < n.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat:
			// tell a deposed leader that it is out of date
			n.send(Message{Type: MsgAppResp, To: m.From, Reject: true, LogIndex: m.LogIndex})
		case MsgPreVote:
			n.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
		return
	}

	if m.Type == MsgVote || m.Type == MsgPreVote {
		n.handleVoteRequest(m)
		return
	}
	switch n.state {
	case Leader:
		n.stepLeader(m)
	case Candidate, PreCandidate:
		n.stepCandidate(m)
	default:
		n.stepFollower(m)
	}
}

func (n *Node) handleVoteRequest(m Message) {
	pre := m.Type == MsgPreVote
	canVote := n.vote == m.From ||
		(n.vote == 0 && n.lead == 0) ||
		(pre && m.Term > n.term)
	respType := MsgVoteResp
	if pre {
		respType = MsgPreVoteResp
	}
	if canVote && n.rlog.isUpToDate(m.LogIndex, m.LogTerm) {
		if !pre {
			n.electionElapsed = 0
			n.vote = m.From
			n.hsDirty = true
		}
		// a pre-vote reply echoes the term it was asked about
		n.send(Message{Type: respType, Term: m.Term, To: m.From})
		return
	}
	n.send(Message{Type: respType, Term: n.term, To: m.From, Reject: true})
}

func (n *Node) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		n.becomeFollower(m.Term, m.From)
		n.stepFollower(m)
	case MsgVoteResp, MsgPreVoteResp:
		want := MsgVoteResp
		if n.state == PreCandidate {
			want = MsgPreVoteResp
		}
		if m.Type != want {
			return
		}
		yes, no := n.poll(m.From, !m.Reject)
		switch {
		case yes >= n.quorum():
			if n.state == PreCandidate {
				n.campaign(false)
			} else {
				n.becomeLeader()
			}
		case no >= n.quorum():
			n.becomeFollower(n.term, 0)
		}
	}
}

func (n *Node) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleAppend(m)
	case MsgHeartbeat:
		n.electionElapsed = 0
		n.lead = m.From
		if c := min(m.Commit, n.rlog.lastIndex()); c > n.commit {
			n.commit = c
		}
		n.send(Message{Type: MsgHeartbeatResp, To: m.From, Round: m.Round})
	case MsgSnap:
		n.electionElapsed = 0
		n.lead = m.From
		n.handleSnapshot(m)
	case MsgTimeoutNow:
		if n.isVoter(n.cfg.ID) && m.From == n.lead {
			n.log.Info("told to take over leadership", "from", m.From)
			n.campaignTransfer()
		}
	}
}

func (n *Node) handleAppend(m Message) {
	if m.LogIndex < n.commit {
		n.send(Message{Type: MsgAppResp, To: m.From, LogIndex: n.commit, Round: m.Round})
		return
	}
	if t, err := n.rlog.term(m.LogIndex); err != nil || t != m.LogTerm {
		ci, ct := n.conflictHint(m.LogIndex)
		n.send(Message{
			Type: MsgAppResp, To: m.From, Reject: true, LogIndex: m.LogIndex,
			ConflictIndex: ci, ConflictTerm: ct, Round: m.Round,
		})
		return
	}

	lastNew := m.LogIndex + uint64(len(m.Entries))
	n.appendEntries(m.Entries)
	if c := min(m.Commit, lastNew); c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppResp, To: m.From, LogIndex: lastNew, Round: m.Round})
}

func (n *Node) appendEntries(ents []Entry) {
	for i, e := range ents {
		if e.Index <= n.rlog.lastIndex() {
			t, err := n.rlog.term(e.Index)
			if err != nil || t == e.Term {
				continue
			}
			if e.Index <= n.commit {
				panic(fmt.Sprintf("raft %d: leader overwrites committed entry %d", n.cfg.ID, e.Index))
			}
			n.rlog.truncateFrom(e.Index)
			if n.confIdx >= e.Index {
				n.recomputeConf()
			}
		}
		n.rlog.append(ents[i:]...)
		for _, ne := range ents[i:] {
			if ne.Type == EntryConfig {
				n.confAppended(ne)
			}
		}
		return
	}
}

// conflictHint lets the leader skip a whole term per round trip instead of
// walking back one entry at a time.
func (n *Node) conflictHint(prev uint64) (index, term uint64) {
	last := n.rlog.lastIndex()
	if prev > last {
		return last + 1, 0
	}
	t, err := n.rlog.term(prev)
	if err != nil {
		return n.commit + 1, 0
	}
	i := prev
	for i > n.rlog.firstIndex() {
		if pt, _ := n.rlog.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i, t
}

func (n *Node) handleSnapshot(m Message) {
	s := m.Snapshot
	if s == nil || s.Index <= n.commit {
		n.send(Message{Type: MsgSnapResp, To: m.From, LogIndex: n.commit})
		return
	}
	if t, err := n.rlog.term(s.Index); err == nil && t == s.Term {
		// we already have this prefix, the snapshot only tells us it is committed
		n.commit = s.Index
		n.send(Message{Type: MsgSnapResp, To: m.From, LogIndex: n.commit})
		return
	}

	if err := n.st.SaveSnapshot(*s); err != nil {
		panic(fmt.Sprintf("raft %d: saving received snapshot failed: %v", n.cfg.ID, err))
	}
	n.log.Info("installing snapshot", "index", s.Index, "term", s.Term, "bytes", len(s.Data))
	n.rlog.restore(s.Index, s.Term)
	n.confBase = slices.Clone(s.Members)
	n.recomputeConf()
	n.commit = s.Index
	n.applySent = s.Index
	select {
	case n.applyc <- applyBatch{snap: s}:
	case <-n.stopc:
		return
	}
	n.send(Message{Type: MsgSnapResp, To: m.From, LogIndex: s.Index})
}

// ---- leader ----

func (n *Node) stepLeader(m Message) {
	p := n.prs[m.From]
	if p == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		p.recentActive = true
		n.ackRound(p, m.Round)
		if m.Reject {
			n.onAppendReject(m, p)
			return
		}
		p.probeSent = false
		p.freeTo(m.LogIndex)
		if m.LogIndex > p.match {
			p.match = m.LogIndex
			p.stuckTicks = 0
			if p.next < p.match+1 {
				p.next = p.match + 1
			}
		}
		if p.state == probe {
			p.becomeReplicate()
		}
		if n.maybeCommit() {
			n.releaseReady()
			n.bcastAppend(true)
		} else {
			n.bcastTo(m.From, false)
		}
		n.maybeTimeoutNow(m.From)
	case MsgHeartbeatResp:
		p.recentActive = true
		n.ackRound(p, m.Round)
		if p.state == probe {
			p.probeSent = false
		}
		if p.state == replicate && len(p.infl) >= n.cfg.MaxInflight {
			p.infl = p.infl[1:]
		}
		if p.match < n.rlog.lastIndex() {
			n.bcastTo(m.From, false)
		}
	case MsgSnapResp:
		p.recentActive = true
		if p.state != snapshotting {
			return
		}
		if m.LogIndex > p.match {
			p.match = m.LogIndex
		}
		p.becomeProbe()
		n.bcastTo(m.From, false)
	}
}

func (n *Node) ackRound(p *progress, r uint64) {
	if r > p.ackedRound {
		p.ackedRound = r
		n.confirmReads()
	}
}

func (n *Node) onAppendReject(m Message, p *progress) {
	if m.Term > n.term {
		return
	}
	switch p.state {
	case replicate:
		if m.LogIndex <= p.match {
			return
		}
		p.becomeProbe()
	case probe:
		if p.next-1 != m.LogIndex {
			return // stale reply to an older probe
		}
	default:
		return
	}

	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if i := n.rlog.lastIndexOfTerm(m.ConflictTerm, m.LogIndex); i > 0 {
			next = i + 1
		}
	}
	next = max(next, p.match+1, 1)
	next = min(next, m.LogIndex)
	p.next = max(next, p.match+1)
	p.probeSent = false
	n.bcastTo(m.From, false)
}

func (n *Node) maybeCommit() bool {
	n.matchBuf = n.matchBuf[:0]
	for id, p := range n.prs {
		if n.isVoter(id) {
			n.matchBuf = append(n.matchBuf, p.match)
		}
	}
	slices.Sort(n.matchBuf)
	q := n.matchBuf[len(n.matchBuf)-n.quorum()]
	if q <= n.commit {
		return false
	}
	if t, err := n.rlog.term(q); err != nil || t != n.term {
		return false
	}
	n.commit = q
	return true
}

func (n *Node) handleProposal(p proposal) {
	if n.state != Leader {
		p.resp <- proposeResp{err: &NotLeaderError{Leader: n.lead}}
		return
	}
	if n.transferee != 0 {
		p.resp <- proposeResp{err: ErrTransferring}
		return
	}
	if p.conf != nil {
		n.handleConfProposal(p)
		return
	}
	if n.rlog.lastIndex()-n.commit >= n.cfg.MaxUncommitted {
		p.resp <- proposeResp{err: ErrBusy}
		return
	}
	idx := n.rlog.lastIndex() + 1
	n.rlog.append(Entry{Index: idx, Term: n.term, Type: EntryNormal, Data: p.data})
	w := n.waiters.add(idx, n.term)
	n.needBcast = true
	p.resp <- proposeResp{w: w, index: idx}
}

func (n *Node) bcastAppend(force bool) {
	for _, id := range n.peers {
		if id != n.cfg.ID {
			n.bcastTo(id, force)
		}
	}
}

// bcastTo keeps sending to one peer until its window is full or it is caught up.
func (n *Node) bcastTo(id NodeID, force bool) {
	for i := 0; i < 32; i++ {
		if !n.sendAppend(id, force && i == 0) {
			return
		}
	}
}

func (n *Node) sendAppend(id NodeID, force bool) bool {
	p := n.prs[id]
	if p == nil || p.paused(n.cfg.MaxInflight) {
		return false
	}
	prev := p.next - 1
	pterm, err := n.rlog.term(prev)
	var ents []Entry
	if err == nil {
		ents, err = n.rlog.slice(p.next, n.cfg.MaxMsgEntries, n.cfg.MaxMsgBytes)
	}
	if err != nil {
		n.sendSnapshot(id, p)
		return false
	}
	if len(ents) == 0 && !force {
		return false
	}

	m := Message{
		Type: MsgApp, To: id, LogIndex: prev, LogTerm: pterm,
		Entries: ents, Commit: n.commit, Round: n.round,
	}
	switch {
	case len(ents) > 0 && p.state == replicate:
		last := ents[len(ents)-1].Index
		p.next = last + 1
		p.infl = append(p.infl, last)
	case p.state == probe:
		p.probeSent = true
	}
	n.send(m)
	return len(ents) > 0
}

func (n *Node) sendSnapshot(id NodeID, p *progress) {
	if p.state == snapshotting {
		return
	}
	p.state = snapshotting
	p.snapTicks = 0
	term := n.term
	go func() {
		s, ok, err := n.st.LoadSnapshot()
		if err == nil && !ok {
			err = fmt.Errorf("no snapshot available")
		}
		if err == nil {
			n.tr.Send(Message{Type: MsgSnap, Term: term, From: n.cfg.ID, To: id, Snapshot: &s})
		} else {
			n.log.Warn("cannot send snapshot", "to", id, "err", err)
		}
		select {
		case n.snapDonec <- snapDone{id: id, err: err}:
		case <-n.stopc:
		}
	}()
}

func (n *Node) handleSnapDone(d snapDone) {
	p := n.prs[d.id]
	if n.state != Leader || p == nil || p.state != snapshotting {
		return
	}
	if d.err != nil {
		p.becomeProbe()
	}
}

func (n *Node) bcastHeartbeat() {
	n.round++
	n.roundQueued = true
	for _, id := range n.peers {
		if id == n.cfg.ID {
			continue
		}
		n.send(Message{
			Type: MsgHeartbeat, To: id, Round: n.round,
			Commit: min(n.commit, n.prs[id].match),
		})
	}
}

// ---- linearizable reads (ReadIndex) ----

func (n *Node) handleRead(r readReq) {
	if n.state != Leader {
		r.resp <- readResp{err: &NotLeaderError{Leader: n.lead}}
		return
	}
	if n.commit < n.leaderNoop {
		// nothing from our own term is committed yet, so commit may be stale
		n.waitReady = append(n.waitReady, &r)
		return
	}
	n.startRead(&r)
}

func (n *Node) startRead(r *readReq) {
	if n.quorum() == 1 {
		r.resp <- readResp{index: n.commit}
		return
	}
	// a heartbeat round that is queued but not yet flushed was still sent
	// after this request arrived, so concurrent reads can share it
	if !n.roundQueued {
		n.bcastHeartbeat()
	}
	n.pendingReads = append(n.pendingReads, pendingRead{req: r, index: n.commit, round: n.round})
}

func (n *Node) releaseReady() {
	if n.commit < n.leaderNoop || len(n.waitReady) == 0 {
		return
	}
	w := n.waitReady
	n.waitReady = nil
	for _, r := range w {
		n.startRead(r)
	}
}

func (n *Node) confirmReads() {
	if len(n.pendingReads) == 0 {
		return
	}
	acks := make([]uint64, 0, len(n.prs))
	for id, p := range n.prs {
		if !n.isVoter(id) {
			continue
		}
		if id == n.cfg.ID {
			acks = append(acks, ^uint64(0))
		} else {
			acks = append(acks, p.ackedRound)
		}
	}
	slices.Sort(acks)
	qr := acks[len(acks)-n.quorum()]
	i := 0
	for i < len(n.pendingReads) && n.pendingReads[i].round <= qr {
		pr := n.pendingReads[i]
		pr.req.resp <- readResp{index: pr.index}
		i++
	}
	n.pendingReads = n.pendingReads[i:]
}

// promoteLag is how many entries a learner may trail by and still be promoted.
const promoteLag = 256

func (n *Node) validateChange(cc ConfChange) error {
	invalid := func(f string, a ...any) error {
		return fmt.Errorf("%w: "+f, append([]any{ErrConfChangeInvalid}, a...)...)
	}
	switch cc.Type {
	case ConfAdd, ConfAddLearner:
		if cc.ID == 0 || hasMember(n.members, cc.ID) {
			return invalid("node %d is already a member", cc.ID)
		}
	case ConfRemove:
		if !hasMember(n.members, cc.ID) {
			return invalid("node %d is not a member", cc.ID)
		}
		if n.isVoter(cc.ID) && len(n.voters) == 1 {
			return invalid("cannot remove the last voter")
		}
	case ConfPromote:
		if !isLearner(n.members, cc.ID) {
			return invalid("node %d is not a learner", cc.ID)
		}
		// match 0 means it never answered, which a short log would otherwise hide
		if p := n.prs[cc.ID]; p == nil || p.match == 0 || n.rlog.lastIndex()-min(p.match, n.rlog.lastIndex()) > promoteLag {
			return ErrLearnerLagging
		}
	default:
		return ErrConfChangeInvalid
	}
	return nil
}

func (n *Node) handleConfProposal(p proposal) {
	cc := *p.conf
	switch {
	case n.commit < n.leaderNoop:
		p.resp <- proposeResp{err: ErrNotReady}
		return
	case n.confIdx > n.commit:
		p.resp <- proposeResp{err: ErrConfChangePending}
		return
	}
	if err := n.validateChange(cc); err != nil {
		p.resp <- proposeResp{err: err}
		return
	}
	idx := n.rlog.lastIndex() + 1
	e := Entry{Index: idx, Term: n.term, Type: EntryConfig, Data: cc.Marshal()}
	n.rlog.append(e)
	w := n.waiters.add(idx, n.term)
	n.confAppended(e)
	n.needBcast = true
	p.resp <- proposeResp{w: w, index: idx}
}
