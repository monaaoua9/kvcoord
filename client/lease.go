package client

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

func (c *Client) Grant(ctx context.Context, ttl time.Duration) (id uint64, granted time.Duration, err error) {
	var resp *kvpb.LeaseGrantResponse
	err = c.write(ctx, func(ctx context.Context, cc *grpc.ClientConn, meta *kvpb.RequestMeta) (err error) {
		resp, err = kvpb.NewLeaseClient(cc).Grant(ctx, &kvpb.LeaseGrantRequest{Meta: meta, TtlMs: uint64(ttl / time.Millisecond)})
		return
	})
	if err != nil {
		return 0, 0, err
	}
	return resp.Id, time.Duration(resp.TtlMs) * time.Millisecond, nil
}

func (c *Client) Revoke(ctx context.Context, id uint64) error {
	return c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) error {
		_, err := kvpb.NewLeaseClient(cc).Revoke(ctx, &kvpb.LeaseRevokeRequest{Id: id})
		return err
	})
}

func (c *Client) TimeToLive(ctx context.Context, id uint64, keys bool) (*kvpb.LeaseTimeToLiveResponse, error) {
	var resp *kvpb.LeaseTimeToLiveResponse
	err := c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) (err error) {
		resp, err = kvpb.NewLeaseClient(cc).TimeToLive(ctx, &kvpb.LeaseTimeToLiveRequest{Id: id, Keys: keys})
		return
	})
	return resp, err
}

// KeepAliveOnce renews a lease and returns its TTL, 0 if it is gone.
func (c *Client) KeepAliveOnce(ctx context.Context, id uint64) (time.Duration, error) {
	var ttl uint64
	err := c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) error {
		stream, err := kvpb.NewLeaseClient(cc).KeepAlive(ctx)
		if err != nil {
			return err
		}
		if err := stream.Send(&kvpb.LeaseKeepAliveRequest{Id: id}); err != nil {
			return err
		}
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		stream.CloseSend()
		ttl = resp.TtlMs
		return nil
	})
	return time.Duration(ttl) * time.Millisecond, err
}

// Session is a lease that renews itself in the background. Done is closed
// when the lease is lost or the session is closed.
type Session struct {
	c      *Client
	id     uint64
	ttl    time.Duration
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (c *Client) NewSession(ctx context.Context, ttl time.Duration) (*Session, error) {
	id, granted, err := c.Grant(ctx, ttl)
	if err != nil {
		return nil, err
	}
	// the renewal loop outlives the setup call, so it gets its own context
	loopCtx, cancel := context.WithCancel(context.Background())
	s := &Session{c: c, id: id, ttl: granted, cancel: cancel, done: make(chan struct{})}
	go s.keepAlive(loopCtx)
	return s, nil
}

func (s *Session) Lease() uint64         { return s.id }
func (s *Session) Done() <-chan struct{} { return s.done }

func (s *Session) keepAlive(ctx context.Context) {
	defer s.finish()
	t := time.NewTicker(s.ttl / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ttl, err := s.c.KeepAliveOnce(ctx, s.id)
		if err != nil {
			continue // transient, keep trying until the server says the lease is gone
		}
		if ttl == 0 {
			return
		}
	}
}

func (s *Session) finish() { s.once.Do(func() { close(s.done) }) }

// Close stops renewing and revokes the lease, releasing its keys and locks.
func (s *Session) Close(ctx context.Context) error {
	s.cancel()
	<-s.done
	err := s.c.Revoke(ctx, s.id)
	if kvpb.Reason(err) == kvpb.ErrLeaseNotFound {
		return nil
	}
	return err
}

// ---- locks ----

// Mutex is a distributed lock owned by a session's lease.
type Mutex struct {
	s     *Session
	name  string
	token uint64
}

func NewMutex(s *Session, name string) *Mutex { return &Mutex{s: s, name: name} }

// Lock blocks until the lock is granted and returns its fencing token. The
// token grows with every grant, so storage that rejects stale tokens is safe
// even if a previous holder pauses and wakes up after its lease expired.
func (m *Mutex) Lock(ctx context.Context) (uint64, error) {
	var resp *kvpb.LockResponse
	err := m.s.c.do(ctx, 0, func(ctx context.Context, cc *grpc.ClientConn) (err error) {
		resp, err = kvpb.NewLockClient(cc).Lock(ctx, &kvpb.LockRequest{Name: []byte(m.name), Lease: m.s.id})
		return
	})
	if err != nil {
		return 0, err
	}
	m.token = resp.FencingToken
	return resp.FencingToken, nil
}

// TryLock returns immediately; acquired is false when somebody else holds it.
func (m *Mutex) TryLock(ctx context.Context) (acquired bool, token uint64, err error) {
	var resp *kvpb.LockResponse
	err = m.s.c.do(ctx, m.s.c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) (err error) {
		resp, err = kvpb.NewLockClient(cc).Lock(ctx, &kvpb.LockRequest{Name: []byte(m.name), Lease: m.s.id, Try: true})
		return
	})
	if err != nil {
		return false, 0, err
	}
	return resp.Acquired, resp.FencingToken, nil
}

func (m *Mutex) Unlock(ctx context.Context) error {
	return m.s.c.do(ctx, m.s.c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) error {
		_, err := kvpb.NewLockClient(cc).Unlock(ctx, &kvpb.UnlockRequest{Name: []byte(m.name), Lease: m.s.id})
		return err
	})
}
