package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/monaaoua9/kvcoord/api/kvpb"
	"github.com/monaaoua9/kvcoord/internal/fsm"
	"github.com/monaaoua9/kvcoord/internal/raft"
	"github.com/monaaoua9/kvcoord/internal/watch"
)

type metrics struct {
	proposals     atomic.Uint64
	proposalErrs  atomic.Uint64
	reads         atomic.Uint64
	leaseExpiries atomic.Uint64
	watchEvents   atomic.Uint64
	lockWaits     atomic.Uint64
}

// ---- kv ----

type kvService struct {
	kvpb.UnimplementedKVServer
	s *Server
}

func (k *kvService) OpenSession(ctx context.Context, _ *kvpb.OpenSessionRequest) (*kvpb.OpenSessionResponse, error) {
	ctx, cancel := k.s.withTimeout(ctx)
	defer cancel()
	res, err := k.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_OpenSession{OpenSession: &kvpb.OpenSessionRequest{}}})
	if err != nil {
		return nil, err
	}
	return &kvpb.OpenSessionResponse{Header: k.s.header(), ClientId: res.GetOpenSession().ClientId}, nil
}

func (k *kvService) Range(ctx context.Context, req *kvpb.RangeRequest) (*kvpb.RangeResponse, error) {
	if len(req.Key) == 0 {
		return nil, kvpb.NewError(codes.InvalidArgument, kvpb.ErrInvalidArgument, "empty key", nil)
	}
	ctx, cancel := k.s.withTimeout(ctx)
	defer cancel()
	if !req.Serializable {
		if err := k.s.linearizableRead(ctx); err != nil {
			return nil, err
		}
	}
	resp, rev, err := k.s.fsm.Range(req)
	if err != nil {
		if errors.Is(err, fsm.ErrCompacted) {
			return nil, kvpb.NewError(codes.OutOfRange, kvpb.ErrCompacted, "revision has been compacted", nil)
		}
		return nil, kvpb.NewError(codes.OutOfRange, kvpb.ErrFutureRevision, "revision is in the future", nil)
	}
	resp.Header = &kvpb.ResponseHeader{NodeId: k.s.cfg.NodeID, Term: k.s.node.Term(), Revision: rev}
	return resp, nil
}

func (k *kvService) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	ctx, cancel := k.s.withTimeout(ctx)
	defer cancel()
	res, err := k.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_Put{Put: req}})
	if err != nil {
		return nil, err
	}
	// results can be shared with the session cache, never mutate them
	return &kvpb.PutResponse{Header: k.s.header(), PrevKv: res.GetPut().PrevKv}, nil
}

func (k *kvService) Compact(ctx context.Context, req *kvpb.CompactRequest) (*kvpb.CompactResponse, error) {
	ctx, cancel := k.s.withTimeout(ctx)
	defer cancel()
	if _, err := k.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_Compact{Compact: req}}); err != nil {
		return nil, err
	}
	return &kvpb.CompactResponse{Header: k.s.header()}, nil
}

func (k *kvService) DeleteRange(ctx context.Context, req *kvpb.DeleteRangeRequest) (*kvpb.DeleteRangeResponse, error) {
	ctx, cancel := k.s.withTimeout(ctx)
	defer cancel()
	res, err := k.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_DeleteRange{DeleteRange: req}})
	if err != nil {
		return nil, err
	}
	r := res.GetDeleteRange()
	return &kvpb.DeleteRangeResponse{Header: k.s.header(), Deleted: r.Deleted, PrevKvs: r.PrevKvs}, nil
}

func (k *kvService) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	ctx, cancel := k.s.withTimeout(ctx)
	defer cancel()
	res, err := k.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_Txn{Txn: req}})
	if err != nil {
		return nil, err
	}
	r := res.GetTxn()
	return &kvpb.TxnResponse{Header: k.s.header(), Succeeded: r.Succeeded, Responses: r.Responses}, nil
}

// ---- lease ----

type leaseService struct {
	kvpb.UnimplementedLeaseServer
	s *Server
}

func (l *leaseService) Grant(ctx context.Context, req *kvpb.LeaseGrantRequest) (*kvpb.LeaseGrantResponse, error) {
	if time.Duration(req.TtlMs)*time.Millisecond < l.s.cfg.MinLeaseTTL {
		return nil, kvpb.NewError(codes.InvalidArgument, kvpb.ErrInvalidArgument, "ttl below the server minimum", nil)
	}
	ctx, cancel := l.s.withTimeout(ctx)
	defer cancel()
	res, err := l.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_LeaseGrant{LeaseGrant: req}})
	if err != nil {
		return nil, err
	}
	g := res.GetLeaseGrant()
	return &kvpb.LeaseGrantResponse{Header: l.s.header(), Id: g.Id, TtlMs: g.TtlMs}, nil
}

func (l *leaseService) Revoke(ctx context.Context, req *kvpb.LeaseRevokeRequest) (*kvpb.LeaseRevokeResponse, error) {
	ctx, cancel := l.s.withTimeout(ctx)
	defer cancel()
	if _, err := l.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_LeaseRevoke{LeaseRevoke: req}}); err != nil {
		return nil, err
	}
	return &kvpb.LeaseRevokeResponse{Header: l.s.header()}, nil
}

func (l *leaseService) KeepAlive(stream kvpb.Lease_KeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		ctx, cancel := l.s.withTimeout(stream.Context())
		res, perr := l.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_LeaseKeepAlive{LeaseKeepAlive: req}})
		cancel()
		if perr != nil {
			return perr
		}
		ka := res.GetLeaseKeepAlive()
		if err := stream.Send(&kvpb.LeaseKeepAliveResponse{Header: l.s.header(), Id: ka.Id, TtlMs: ka.TtlMs}); err != nil {
			return err
		}
	}
}

func (l *leaseService) TimeToLive(ctx context.Context, req *kvpb.LeaseTimeToLiveRequest) (*kvpb.LeaseTimeToLiveResponse, error) {
	ctx, cancel := l.s.withTimeout(ctx)
	defer cancel()
	if err := l.s.linearizableRead(ctx); err != nil {
		return nil, err
	}
	resp := &kvpb.LeaseTimeToLiveResponse{Header: l.s.header(), Id: req.Id, RemainingMs: -1}
	if info, ok := l.s.fsm.LeaseInfo(req.Id, req.Keys); ok {
		resp.GrantedTtlMs = uint64(info.TTLNs / int64(time.Millisecond))
		resp.RemainingMs = (info.DeadlineNs - time.Now().UnixNano()) / int64(time.Millisecond)
		resp.Keys = info.Keys
	}
	return resp, nil
}

// ---- lock ----

type lockService struct {
	kvpb.UnimplementedLockServer
	s *Server
}

func (l *lockService) Lock(ctx context.Context, req *kvpb.LockRequest) (*kvpb.LockResponse, error) {
	if len(req.Name) == 0 || req.Lease == 0 {
		return nil, kvpb.NewError(codes.InvalidArgument, kvpb.ErrInvalidArgument, "name and lease are required", nil)
	}
	// subscribe first: a grant can land the instant the command is applied
	events, unsub := l.s.fsm.SubscribeLock(string(req.Name), req.Lease)
	defer unsub()

	pctx, cancel := l.s.withTimeout(ctx)
	res, err := l.s.propose(pctx, &kvpb.Command{Body: &kvpb.Command_Lock{Lock: req}})
	cancel()
	if err != nil {
		return nil, err
	}
	lr := res.GetLock()
	if lr.Acquired {
		return &kvpb.LockResponse{Header: l.s.header(), Acquired: true, FencingToken: lr.FencingToken}, nil
	}
	if req.Try {
		return &kvpb.LockResponse{Header: l.s.header()}, nil
	}

	l.s.m.lockWaits.Add(1)
	select {
	case ev := <-events:
		if ev.Err != "" {
			code := codes.Unavailable
			if ev.Err == kvpb.ErrLeaseExpired {
				code = codes.NotFound
			}
			return nil, kvpb.NewError(code, ev.Err, "lock wait aborted", nil)
		}
		return &kvpb.LockResponse{Header: l.s.header(), Acquired: true, FencingToken: ev.Token}, nil
	case <-ctx.Done():
		// leave the queue, otherwise the lock would be granted to a caller that went away
		bg, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		l.s.propose(bg, &kvpb.Command{Body: &kvpb.Command_Unlock{Unlock: &kvpb.UnlockRequest{Name: req.Name, Lease: req.Lease}}})
		return nil, l.s.mapErr(ctx.Err())
	}
}

func (l *lockService) Unlock(ctx context.Context, req *kvpb.UnlockRequest) (*kvpb.UnlockResponse, error) {
	ctx, cancel := l.s.withTimeout(ctx)
	defer cancel()
	if _, err := l.s.propose(ctx, &kvpb.Command{Body: &kvpb.Command_Unlock{Unlock: req}}); err != nil {
		return nil, err
	}
	return &kvpb.UnlockResponse{Header: l.s.header()}, nil
}

// ---- cluster ----

type clusterService struct {
	kvpb.UnimplementedClusterServer
	s *Server
}

func (c *clusterService) Status(ctx context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	st := c.s.node.Status()
	fs := c.s.fsm.Stats()
	return &kvpb.StatusResponse{
		Header:        c.s.header(),
		LeaderId:      st.Leader,
		State:         st.State.String(),
		CommitIndex:   st.Commit,
		AppliedIndex:  st.Applied,
		LastIndex:     st.LastIndex,
		FirstIndex:    st.FirstIndex,
		SnapshotIndex: st.SnapIndex,
		Keys:          uint64(fs.Keys),
		Leases:        uint64(fs.Leases),
		Locks:         uint64(fs.Locks),
		Watchers:      uint64(fs.Watchers),
	}, nil
}

// ---- watch ----

type watchService struct {
	kvpb.UnimplementedWatchServer
	s *Server
}

func (w *watchService) Watch(stream kvpb.Watch_WatchServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	out := make(chan *kvpb.WatchResponse, 64)
	sendErr := make(chan error, 1)
	go func() {
		for {
			select {
			case r := <-out:
				if err := stream.Send(r); err != nil {
					sendErr <- err
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	emit := func(r *kvpb.WatchResponse) bool {
		r.Header = w.s.header()
		select {
		case out <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}

	var mu sync.Mutex
	active := make(map[uint64]*watch.Watcher)
	defer func() {
		mu.Lock()
		for _, wt := range active {
			w.s.fsm.CancelWatcher(wt)
		}
		mu.Unlock()
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			select {
			case e := <-sendErr:
				return e
			default:
			}
			return nil
		}

		switch r := req.Request.(type) {
		case *kvpb.WatchRequest_Create:
			c := r.Create
			if len(c.Key) == 0 {
				emit(&kvpb.WatchResponse{Canceled: true, CancelReason: kvpb.ErrInvalidArgument})
				continue
			}
			wt, compactRev, err := w.s.fsm.Watch(fsm.WatchSpec{
				Key: c.Key, End: c.RangeEnd, StartRev: c.StartRevision, PrevKV: c.PrevKv,
			}, w.s.cfg.WatchBuffer)
			if err != nil {
				emit(&kvpb.WatchResponse{Canceled: true, CancelReason: kvpb.ErrCompacted, CompactRevision: compactRev})
				continue
			}
			mu.Lock()
			active[wt.ID] = wt
			mu.Unlock()
			emit(&kvpb.WatchResponse{WatchId: wt.ID, Created: true})

			go func() {
				for b := range wt.C {
					w.s.m.watchEvents.Add(uint64(len(b.Events)))
					if !emit(&kvpb.WatchResponse{WatchId: wt.ID, Events: b.Events}) {
						return
					}
				}
				mu.Lock()
				delete(active, wt.ID)
				mu.Unlock()
				emit(&kvpb.WatchResponse{WatchId: wt.ID, Canceled: true, CancelReason: wt.Reason()})
			}()

		case *kvpb.WatchRequest_Cancel:
			mu.Lock()
			wt := active[r.Cancel.WatchId]
			mu.Unlock()
			if wt != nil {
				w.s.fsm.CancelWatcher(wt)
			}
		}
	}
}

func (c *clusterService) members() []*kvpb.Member {
	leader := c.s.node.Status().Leader
	var out []*kvpb.Member
	for _, m := range c.s.node.Members() {
		out = append(out, &kvpb.Member{Id: m.ID, Addr: m.Addr, IsLeader: m.ID == leader, Learner: m.Learner})
	}
	return out
}

func (c *clusterService) MemberList(context.Context, *kvpb.MemberListRequest) (*kvpb.MemberListResponse, error) {
	return &kvpb.MemberListResponse{Header: c.s.header(), Members: c.members()}, nil
}

func (c *clusterService) MemberAdd(ctx context.Context, req *kvpb.MemberAddRequest) (*kvpb.MemberListResponse, error) {
	if req.Id == 0 || req.Addr == "" {
		return nil, kvpb.NewError(codes.InvalidArgument, kvpb.ErrInvalidArgument, "id and addr are required", nil)
	}
	ctx, cancel := c.s.withTimeout(ctx)
	defer cancel()
	cc := raft.ConfChange{Type: raft.ConfAdd, ID: req.Id, Addr: req.Addr}
	if req.Learner {
		cc.Type = raft.ConfAddLearner
	}
	if err := c.s.node.ProposeConfChange(ctx, cc); err != nil {
		return nil, c.s.mapErr(err)
	}
	return &kvpb.MemberListResponse{Header: c.s.header(), Members: c.members()}, nil
}

func (c *clusterService) MemberPromote(ctx context.Context, req *kvpb.MemberPromoteRequest) (*kvpb.MemberListResponse, error) {
	ctx, cancel := c.s.withTimeout(ctx)
	defer cancel()
	if err := c.s.node.ProposeConfChange(ctx, raft.ConfChange{Type: raft.ConfPromote, ID: req.Id}); err != nil {
		return nil, c.s.mapErr(err)
	}
	return &kvpb.MemberListResponse{Header: c.s.header(), Members: c.members()}, nil
}

func (c *clusterService) TransferLeader(ctx context.Context, req *kvpb.TransferLeaderRequest) (*kvpb.TransferLeaderResponse, error) {
	ctx, cancel := c.s.withTimeout(ctx)
	defer cancel()
	if err := c.s.StepDown(ctx, req.Target); err != nil {
		return nil, c.s.mapErr(err)
	}
	return &kvpb.TransferLeaderResponse{Header: c.s.header()}, nil
}

func (c *clusterService) MemberRemove(ctx context.Context, req *kvpb.MemberRemoveRequest) (*kvpb.MemberListResponse, error) {
	ctx, cancel := c.s.withTimeout(ctx)
	defer cancel()
	if err := c.s.node.ProposeConfChange(ctx, raft.ConfChange{Type: raft.ConfRemove, ID: req.Id}); err != nil {
		return nil, c.s.mapErr(err)
	}
	return &kvpb.MemberListResponse{Header: c.s.header(), Members: c.members()}, nil
}
