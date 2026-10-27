package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	ID NodeID
	// Peers is the initial voter set of a brand new cluster, including ID. It is
	// only used to bootstrap an empty log; afterwards membership comes from the log.
	Peers []NodeID
	Addrs map[NodeID]string // addresses for Peers, handed to the transport via OnConfig
	// Join starts an empty node that waits for a leader to add it.
	Join bool
	// OnConfig is called from the event loop whenever the effective voter set
	// changes (and once at startup). It must not block.
	OnConfig func([]Member)

	ElectionTick  int // ticks without a leader before campaigning; actual timeout is in [ET, 2*ET)
	HeartbeatTick int
	TickInterval  time.Duration

	MaxMsgEntries  int
	MaxMsgBytes    int
	MaxInflight    int
	MaxUncommitted uint64

	// SnapshotEvery is the number of applied entries between snapshots, 0 disables them.
	SnapshotEvery    uint64
	SnapshotTrailing uint64 // entries kept in memory below the snapshot point

	DisablePreVote     bool
	DisableCheckQuorum bool

	Logger *slog.Logger
}

func (c *Config) validate() error {
	if c.ID == 0 {
		return errors.New("raft: zero node id")
	}
	if !c.Join && !slices.Contains(c.Peers, c.ID) {
		return errors.New("raft: peers must include the local node")
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.HeartbeatTick >= c.ElectionTick {
		return errors.New("raft: election tick must be larger than heartbeat tick")
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.MaxMsgEntries == 0 {
		c.MaxMsgEntries = 256
	}
	if c.MaxMsgBytes == 0 {
		c.MaxMsgBytes = 1 << 20
	}
	if c.MaxInflight == 0 {
		c.MaxInflight = 64
	}
	if c.MaxUncommitted == 0 {
		c.MaxUncommitted = 16384
	}
	if c.SnapshotEvery > 0 && c.SnapshotTrailing == 0 {
		c.SnapshotTrailing = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

type transferReq struct {
	target NodeID
	resp   chan error
}

type proposal struct {
	data []byte
	conf *ConfChange
	resp chan proposeResp
}

type proposeResp struct {
	w     *waiter
	index uint64
	err   error
}

type readReq struct {
	resp chan readResp
}

type readResp struct {
	index uint64
	err   error
}

type pendingRead struct {
	req   *readReq
	index uint64
	round uint64
}

type applyBatch struct {
	ents []Entry
	snap *Snapshot
}

type snapDone struct {
	id  NodeID
	err error
}

type Node struct {
	cfg             Config
	log             *slog.Logger
	st              Storage
	fsm             StateMachine
	tr              Transport
	transferee      NodeID // event loop only: a transfer to this node is in flight
	transferElapsed int
	peers           []NodeID // event loop only, every member: they all get the log
	voters          []NodeID // the subset that votes and counts toward quorums

	membersMu  sync.Mutex
	memberSnap []Member // copy of members for other goroutines

	recvc     chan Message
	propc     chan proposal
	readc     chan readReq
	statc     chan chan Status
	transc    chan transferReq
	snapDonec chan snapDone
	compactc  chan uint64
	appliedc  chan struct{}
	applyc    chan applyBatch
	stopc     chan struct{}
	loopDone  chan struct{}
	applyDone chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once

	waiters   waitMap
	applied   appliedWaiters
	lastSnap  atomic.Uint64 // index of the newest snapshot taken or restored
	snapBusy  atomic.Bool   // a background snapshot is being written
	snapWG    sync.WaitGroup
	dropped   atomic.Uint64
	stateNow  atomic.Uint32
	leaderNow atomic.Uint64
	termNow   atomic.Uint64

	// everything below belongs to the event loop
	state   StateType
	term    uint64
	vote    NodeID
	lead    NodeID
	rlog    *raftLog
	commit  uint64
	hsDirty bool

	votes            map[NodeID]bool
	prs              map[NodeID]*progress
	electionElapsed  int
	heartbeatElapsed int
	randElection     int
	round            uint64
	roundQueued      bool
	leaderNoop       uint64
	members          []Member
	confBase         []Member // voter set at the start of the in-memory log
	confIdx          uint64   // index of the newest config entry in the log, 0 if none
	aconf            []Member // apply goroutine: voter set as of the applied index
	needBcast        bool
	pendingReads     []pendingRead
	waitReady        []*readReq
	applySent        uint64
	outbox           []Message
	matchBuf         []uint64
	rng              *rand.Rand
}

func NewNode(cfg Config, st Storage, fsm StateMachine, tr Transport) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	rec, err := st.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	n := &Node{
		cfg:       cfg,
		log:       cfg.Logger.With("node", cfg.ID),
		st:        st,
		fsm:       fsm,
		tr:        tr,
		recvc:     make(chan Message, 4096),
		propc:     make(chan proposal, 1024),
		readc:     make(chan readReq, 1024),
		statc:     make(chan chan Status),
		transc:    make(chan transferReq),
		snapDonec: make(chan snapDone, 16),
		compactc:  make(chan uint64, 4),
		appliedc:  make(chan struct{}, 1),
		applyc:    make(chan applyBatch, 16),
		stopc:     make(chan struct{}),
		loopDone:  make(chan struct{}),
		applyDone: make(chan struct{}),
		rng:       rand.New(rand.NewPCG(uint64(cfg.ID), uint64(time.Now().UnixNano()))),
	}
	n.term, n.vote = rec.HardState.Term, rec.HardState.Vote
	n.rlog = newLog(rec.SnapIndex, rec.SnapTerm, rec.Entries)
	n.commit = rec.SnapIndex
	n.applySent = rec.SnapIndex
	n.lastSnap.Store(rec.SnapIndex)
	n.applied.set(rec.SnapIndex)

	if rec.SnapIndex > 0 {
		s, ok, err := st.LoadSnapshot()
		if err != nil || !ok {
			return nil, fmt.Errorf("raft: snapshot at %d missing: %v", rec.SnapIndex, err)
		}
		if err := fsm.Restore(s.Data); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
		n.confBase = slices.Clone(s.Members)
		n.aconf = slices.Clone(s.Members)
	}

	if rec.SnapIndex == 0 && len(rec.Entries) == 0 && rec.HardState.Term == 0 && !cfg.Join {
		n.bootstrap()
	}
	n.recomputeConf()
	n.resetTimeout()
	return n, nil
}

// bootstrap writes the initial voter set into the log as ordinary config
// entries, identical on every founding node, so that a node added later
// learns the full membership just by replicating the log.
func (n *Node) bootstrap() {
	ids := slices.Clone(n.cfg.Peers)
	slices.Sort(ids)
	n.term = 1
	n.hsDirty = true
	for i, id := range ids {
		cc := ConfChange{Type: ConfAdd, ID: id, Addr: n.cfg.Addrs[id]}
		n.rlog.append(Entry{Index: uint64(i + 1), Term: 1, Type: EntryConfig, Data: cc.Marshal()})
	}
	n.commit = uint64(len(ids))
}

func (n *Node) Start() {
	n.startOnce.Do(func() {
		go n.applyLoop()
		go n.run()
	})
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopc) })
	n.startOnce.Do(func() { close(n.loopDone); close(n.applyDone) })
	<-n.loopDone
	<-n.applyDone
	n.snapWG.Wait()
}

// Step feeds a message received from the network into the node.
func (n *Node) Step(m Message) {
	select {
	case n.recvc <- m:
	default:
		n.dropped.Add(1)
	}
}

func (n *Node) Dropped() uint64 { return n.dropped.Load() }

func (n *Node) AppliedIndex() uint64 { return n.applied.get() }

// IsLeader is a cheap, possibly stale hint for routing decisions.
func (n *Node) IsLeader() bool { return StateType(n.stateNow.Load()) == Leader }

func (n *Node) LeaderHint() NodeID { return n.leaderNow.Load() }

func (n *Node) Term() uint64 { return n.termNow.Load() }

func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	req := proposal{data: data, resp: make(chan proposeResp, 1)}
	select {
	case n.propc <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopc:
		return nil, ErrStopped
	}
	var pr proposeResp
	select {
	case pr = <-req.resp:
	case <-n.stopc:
		return nil, ErrStopped
	}
	if pr.err != nil {
		return nil, pr.err
	}
	select {
	case r := <-pr.w.ch:
		return r.val, r.err
	case <-ctx.Done():
		n.waiters.cancel(pr.index)
		return nil, ctx.Err()
	case <-n.stopc:
		return nil, ErrStopped
	}
}

// ReadIndex returns an index such that, once the state machine has applied
// it, a local read is linearizable. Only the leader can serve it.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	req := readReq{resp: make(chan readResp, 1)}
	select {
	case n.readc <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopc:
		return 0, ErrStopped
	}
	select {
	case r := <-req.resp:
		return r.index, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopc:
		return 0, ErrStopped
	}
}

func (n *Node) WaitApplied(ctx context.Context, index uint64) error {
	select {
	case <-n.applied.wait(index):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
}

func (n *Node) Status() Status {
	c := make(chan Status, 1)
	select {
	case n.statc <- c:
		return <-c
	case <-n.stopc:
		return Status{ID: n.cfg.ID}
	}
}

func (n *Node) run() {
	defer close(n.loopDone)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopc:
			n.waiters.failAll(ErrStopped)
			return
		case <-ticker.C:
			n.tick()
		case m := <-n.recvc:
			n.step(m)
		case p := <-n.propc:
			n.handleProposal(p)
		case r := <-n.readc:
			n.handleRead(r)
		case c := <-n.statc:
			c <- n.status()
		case t := <-n.transc:
			n.handleTransfer(t)
		case d := <-n.snapDonec:
			n.handleSnapDone(d)
		case idx := <-n.compactc:
			n.handleCompact(idx)
		case <-n.appliedc:
		}
		n.drain()
		n.ready()
	}
}

// drain pulls whatever else is already queued so one fsync covers a batch.
func (n *Node) drain() {
	for i := 0; i < 128; i++ {
		select {
		case m := <-n.recvc:
			n.step(m)
		case p := <-n.propc:
			n.handleProposal(p)
		case r := <-n.readc:
			n.handleRead(r)
		default:
			return
		}
	}
}

func (n *Node) ready() {
	if n.hsDirty || len(n.rlog.unstable()) > 0 {
		var hs *HardState
		if n.hsDirty {
			hs = &HardState{Term: n.term, Vote: n.vote}
		}
		if err := n.st.Save(hs, n.rlog.unstable()); err != nil {
			panic(fmt.Sprintf("raft %d: storage write failed: %v", n.cfg.ID, err))
		}
		n.rlog.markStable()
		n.hsDirty = false
	}

	if n.state == Leader {
		if self := n.prs[n.cfg.ID]; self != nil {
			self.match = n.rlog.lastIndex()
			self.next = self.match + 1
		}
		advanced := n.maybeCommit()
		if advanced {
			n.releaseReady()
		}
		if advanced || n.needBcast {
			n.bcastAppend(advanced)
			n.needBcast = false
		}
		if advanced && !hasMember(n.members, n.cfg.ID) && n.commit >= n.confIdx {
			n.log.Info("removed from the cluster, stepping down")
			n.becomeFollower(n.term, 0)
		}
	}

	n.dispatchApply()

	for i := range n.outbox {
		n.tr.Send(n.outbox[i])
	}
	clear(n.outbox)
	n.outbox = n.outbox[:0]
	n.roundQueued = false

	n.stateNow.Store(uint32(n.state))
	n.leaderNow.Store(n.lead)
	n.termNow.Store(n.term)
}

func (n *Node) dispatchApply() {
	for n.applySent < n.commit {
		hi := min(n.commit, n.applySent+512)
		ents, err := n.rlog.slice(n.applySent+1, int(hi-n.applySent), 1<<30)
		if err != nil {
			panic(fmt.Sprintf("raft %d: cannot slice committed range: %v", n.cfg.ID, err))
		}
		select {
		case n.applyc <- applyBatch{ents: ents}:
			n.applySent = ents[len(ents)-1].Index
		default:
			return
		}
	}
}

func (n *Node) applyLoop() {
	defer close(n.applyDone)
	for {
		select {
		case <-n.stopc:
			return
		case b := <-n.applyc:
			if b.snap != nil {
				if err := n.fsm.Restore(b.snap.Data); err != nil {
					panic(fmt.Sprintf("raft %d: restore failed: %v", n.cfg.ID, err))
				}
				n.aconf = slices.Clone(b.snap.Members)
				n.lastSnap.Store(b.snap.Index)
				n.applied.set(b.snap.Index)
				n.wake()
				continue
			}
			for _, e := range b.ents {
				var res any
				switch e.Type {
				case EntryNormal:
					res = n.fsm.Apply(e)
				case EntryConfig:
					if cc, err := UnmarshalConfChange(e.Data); err == nil {
						n.aconf = applyChange(n.aconf, cc)
					}
				}
				n.waiters.resolve(e.Index, e.Term, res)
			}
			last := b.ents[len(b.ents)-1]
			n.applied.set(last.Index)
			n.maybeSnapshot(last)
			n.wake()
		}
	}
}

func (n *Node) wake() {
	select {
	case n.appliedc <- struct{}{}:
	default:
	}
}

func (n *Node) maybeSnapshot(last Entry) {
	if n.cfg.SnapshotEvery == 0 || last.Index-n.lastSnap.Load() < n.cfg.SnapshotEvery {
		return
	}
	if as, ok := n.fsm.(AsyncSnapshotter); ok {
		n.snapshotInBackground(as, last)
		return
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		n.log.Error("fsm snapshot failed", "err", err)
		return
	}
	if err := n.st.SaveSnapshot(Snapshot{Index: last.Index, Term: last.Term, Members: slices.Clone(n.aconf), Data: data}); err != nil {
		n.log.Error("saving snapshot failed", "err", err)
		return
	}
	n.lastSnap.Store(last.Index)
	n.log.Info("snapshot taken", "index", last.Index, "bytes", len(data))
	select {
	case n.compactc <- last.Index:
	default:
	}
}

// snapshotInBackground captures the state here, on the apply goroutine, and lets a
// worker serialise and persist it so applying carries on meanwhile. One at a time:
// if the previous one is still running the next batch simply tries again.
func (n *Node) snapshotInBackground(as AsyncSnapshotter, last Entry) {
	if !n.snapBusy.CompareAndSwap(false, true) {
		return
	}
	write := as.SnapshotAsync()
	members := slices.Clone(n.aconf)
	n.snapWG.Add(1)
	go func() {
		defer n.snapWG.Done()
		defer n.snapBusy.Store(false)
		data, err := write()
		if err != nil {
			n.log.Error("fsm snapshot failed", "err", err)
			return
		}
		// storage ignores a snapshot older than one it already has (a received one
		// may have overtaken us)
		if err := n.st.SaveSnapshot(Snapshot{Index: last.Index, Term: last.Term, Members: members, Data: data}); err != nil {
			n.log.Error("saving snapshot failed", "err", err)
			return
		}
		for old := n.lastSnap.Load(); old < last.Index; old = n.lastSnap.Load() {
			if n.lastSnap.CompareAndSwap(old, last.Index) {
				break
			}
		}
		n.log.Info("snapshot taken", "index", last.Index, "bytes", len(data))
		select {
		case n.compactc <- last.Index:
		default:
		}
	}()
}

func (n *Node) handleCompact(snapIdx uint64) {
	if snapIdx <= n.cfg.SnapshotTrailing {
		return
	}
	target := snapIdx - n.cfg.SnapshotTrailing
	if target <= n.rlog.snapIndex {
		return
	}
	// the new base must carry every config change that is about to leave memory
	for _, e := range n.rlog.ents {
		if e.Index > target {
			break
		}
		if e.Type == EntryConfig {
			if cc, err := UnmarshalConfChange(e.Data); err == nil {
				n.confBase = applyChange(n.confBase, cc)
			}
		}
	}
	n.rlog.compactTo(target)
}

func (n *Node) quorum() int { return len(n.voters)/2 + 1 }

func (n *Node) isVoter(id NodeID) bool { return slices.Contains(n.voters, id) }

func (n *Node) resetTimeout() {
	n.randElection = n.cfg.ElectionTick + n.rng.IntN(n.cfg.ElectionTick)
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	if m.Term == 0 {
		m.Term = n.term
	}
	n.outbox = append(n.outbox, m)
}

func (n *Node) status() Status {
	s := Status{
		ID:           n.cfg.ID,
		Term:         n.term,
		Leader:       n.lead,
		State:        n.state,
		Commit:       n.commit,
		Applied:      n.applied.get(),
		LastIndex:    n.rlog.lastIndex(),
		FirstIndex:   n.rlog.firstIndex(),
		SnapIndex:    n.rlog.snapIndex,
		LeaderReady:  n.state == Leader && n.commit >= n.leaderNoop,
		Transferring: n.transferee != 0,
	}
	if n.state == Leader {
		s.Match = make(map[NodeID]uint64, len(n.prs))
		for id, p := range n.prs {
			s.Match[id] = p.match
		}
	}
	return s
}

func (n *Node) tick() {
	n.electionElapsed++
	if n.state == Leader {
		n.tickLeader()
		return
	}
	if n.electionElapsed >= n.randElection {
		n.electionElapsed = 0
		if !n.isVoter(n.cfg.ID) {
			return // joining, removed and learner nodes never campaign
		}
		n.campaign(!n.cfg.DisablePreVote && len(n.voters) > 1)
	}
}

func (n *Node) tickLeader() {
	n.heartbeatElapsed++
	if n.transferee != 0 {
		n.transferElapsed++
		if n.transferElapsed >= n.cfg.ElectionTick {
			n.log.Warn("leadership transfer timed out", "target", n.transferee)
			n.transferee = 0
		}
	}
	if !n.cfg.DisableCheckQuorum && n.electionElapsed >= n.cfg.ElectionTick {
		n.electionElapsed = 0
		if !n.quorumActive() {
			n.log.Warn("lost contact with a quorum, stepping down", "term", n.term)
			n.becomeFollower(n.term, 0)
			return
		}
	}
	if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
		n.heartbeatElapsed = 0
		n.bcastHeartbeat()
		n.tickProgress()
	}
}

func (n *Node) quorumActive() bool {
	active := 0
	for id, p := range n.prs {
		if n.isVoter(id) && (id == n.cfg.ID || p.recentActive) {
			active++
		}
		p.recentActive = false
	}
	return active >= n.quorum()
}

// tickProgress rescues followers whose pipelined appends or snapshot got lost.
func (n *Node) tickProgress() {
	for id, p := range n.prs {
		if id == n.cfg.ID {
			continue
		}
		switch p.state {
		case replicate:
			if p.match < p.next-1 {
				p.stuckTicks++
				if p.stuckTicks >= 5 {
					p.stuckTicks = 0
					p.becomeProbe()
					n.sendAppend(id, false)
				}
			} else {
				p.stuckTicks = 0
			}
		case snapshotting:
			p.snapTicks++
			if p.snapTicks > 4*n.cfg.ElectionTick {
				p.becomeProbe()
			}
		}
	}
}

// Members returns the voter set currently in effect on this node.
func (n *Node) Members() []Member {
	n.membersMu.Lock()
	defer n.membersMu.Unlock()
	return slices.Clone(n.memberSnap)
}

// recomputeConf rebuilds the voter set from the base plus every config entry
// still in the log. Needed after truncation, snapshot install and startup.
func (n *Node) recomputeConf() {
	ms := slices.Clone(n.confBase)
	n.confIdx = 0
	for _, e := range n.rlog.ents {
		if e.Type != EntryConfig {
			continue
		}
		if cc, err := UnmarshalConfChange(e.Data); err == nil {
			ms = applyChange(ms, cc)
			n.confIdx = e.Index
		}
	}
	n.installMembers(ms)
}

// confAppended applies a config entry the moment it enters the log. Raft
// requires this (not waiting for commit) for single-server changes to be safe.
func (n *Node) confAppended(e Entry) {
	cc, err := UnmarshalConfChange(e.Data)
	if err != nil {
		return
	}
	n.confIdx = e.Index
	n.installMembers(applyChange(n.members, cc))
}

func (n *Node) installMembers(ms []Member) {
	if slices.Equal(ms, n.members) {
		return
	}
	n.members = ms
	n.peers = memberIDs(ms)
	n.voters = voterIDs(ms)

	n.membersMu.Lock()
	n.memberSnap = slices.Clone(ms)
	n.membersMu.Unlock()

	if n.state == Leader {
		last := n.rlog.lastIndex()
		for _, id := range n.peers {
			if n.prs[id] == nil {
				n.prs[id] = &progress{next: last + 1}
			}
		}
		for id := range n.prs {
			if !hasMember(ms, id) {
				delete(n.prs, id)
			}
		}
		n.needBcast = true
	}
	n.log.Info("membership changed", "members", n.peers, "voters", n.voters, "conf_index", n.confIdx)
	if n.cfg.OnConfig != nil {
		n.cfg.OnConfig(slices.Clone(ms))
	}
}

// ProposeConfChange adds or removes one voter and waits until the change is
// committed. Only one change may be in flight; callers retry on
// ErrConfChangePending.
func (n *Node) ProposeConfChange(ctx context.Context, cc ConfChange) error {
	req := proposal{conf: &cc, resp: make(chan proposeResp, 1)}
	select {
	case n.propc <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
	var pr proposeResp
	select {
	case pr = <-req.resp:
	case <-n.stopc:
		return ErrStopped
	}
	if pr.err != nil {
		return pr.err
	}
	select {
	case r := <-pr.w.ch:
		return r.err
	case <-ctx.Done():
		n.waiters.cancel(pr.index)
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
}

// TransferLeadership hands leadership to target, a voter, and returns once some
// node other than this one is leading. New proposals fail with ErrTransferring
// meanwhile, so callers retry through the new leader.
func (n *Node) TransferLeadership(ctx context.Context, target NodeID) error {
	req := transferReq{target: target, resp: make(chan error, 1)}
	select {
	case n.transc <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
	if err := <-req.resp; err != nil {
		return err
	}
	t := time.NewTicker(n.cfg.TickInterval)
	defer t.Stop()
	for {
		if l := n.LeaderHint(); l != 0 && l != n.cfg.ID && !n.IsLeader() {
			return nil
		}
		select {
		case <-t.C:
			if st := n.Status(); st.State == Leader && !st.Transferring {
				return ErrTransferTimedOut
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopc:
			return ErrStopped
		}
	}
}
