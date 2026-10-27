// Package client is the Go client for the kvcoord cluster. It follows leader
// redirects, retries transient failures and makes writes exactly-once through
// a server-side session.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

var (
	ErrClosed        = errors.New("client: closed")
	ErrNoEndpoints   = errors.New("client: no endpoints configured")
	ErrSessionLost   = errors.New("client: write session expired, outcome of the in-flight request is unknown")
	ErrLeaseLost     = errors.New("client: lease no longer exists")
	ErrWatchCompact  = errors.New("client: watch start revision was compacted")
	errNoLeaderRetry = errors.New("retry")
)

type Config struct {
	Endpoints      []string
	Creds          credentials.TransportCredentials
	RequestTimeout time.Duration // per attempt; the caller's context bounds the whole call
}

type Client struct {
	cfg Config

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	leader int // index into Endpoints, best guess
	closed bool

	sessMu   sync.Mutex
	clientID uint64
	seq      uint64
	inflight map[uint64]struct{}
}

func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 3 * time.Second
	}
	if cfg.Creds == nil {
		cfg.Creds = insecure.NewCredentials()
	}
	return &Client{cfg: cfg, conns: make(map[string]*grpc.ClientConn), inflight: make(map[uint64]struct{})}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var first error
	for _, cc := range c.conns {
		if err := cc.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.conns = nil
	return first
}

func (c *Client) endpoint() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.Endpoints[c.leader%len(c.cfg.Endpoints)]
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if cc := c.conns[addr]; cc != nil {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(c.cfg.Creds))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

func (c *Client) rotate() {
	c.mu.Lock()
	c.leader++
	c.mu.Unlock()
}

// setLeader remembers addr as the leader; addresses we were not configured
// with are learned from the redirect.
func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, e := range c.cfg.Endpoints {
		if e == addr {
			c.leader = i
			return
		}
	}
	c.cfg.Endpoints = append(c.cfg.Endpoints, addr)
	c.leader = len(c.cfg.Endpoints) - 1
}

func retryable(err error) bool {
	switch kvpb.Reason(err) {
	case kvpb.ErrNotLeader, kvpb.ErrNoLeader, kvpb.ErrUnavailable, kvpb.ErrBusy, kvpb.ErrTimeout:
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// do runs fn against the believed leader until it succeeds, fails for good,
// or ctx ends. attemptTimeout of 0 means attempts are bounded only by ctx.
func (c *Client) do(ctx context.Context, attemptTimeout time.Duration, fn func(context.Context, *grpc.ClientConn) error) error {
	backoff := 10 * time.Millisecond
	for attempt := 0; ; attempt++ {
		cc, err := c.conn(c.endpoint())
		if err != nil {
			return err
		}
		actx, cancel := ctx, context.CancelFunc(func() {})
		if attemptTimeout > 0 {
			actx, cancel = context.WithTimeout(ctx, attemptTimeout)
		}
		err = fn(actx, cc)
		cancel()
		if err == nil {
			return nil
		}
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("%w (last error: %v)", cerr, err)
		}
		if !retryable(err) {
			return err
		}

		if kvpb.Reason(err) == kvpb.ErrNotLeader {
			if addr := kvpb.Metadata(err)[kvpb.MetaLeaderAddr]; addr != "" {
				c.setLeader(addr)
				continue
			}
		}
		c.rotate()
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %v)", ctx.Err(), err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 400*time.Millisecond)
	}
}

// ---- session / request ids ----

func (c *Client) ensureSession(ctx context.Context) (uint64, error) {
	c.sessMu.Lock()
	id := c.clientID
	c.sessMu.Unlock()
	if id != 0 {
		return id, nil
	}
	var resp *kvpb.OpenSessionResponse
	err := c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) (err error) {
		resp, err = kvpb.NewKVClient(cc).OpenSession(ctx, &kvpb.OpenSessionRequest{})
		return
	})
	if err != nil {
		return 0, err
	}
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	if c.clientID == 0 {
		c.clientID = resp.ClientId
	}
	return c.clientID, nil
}

func (c *Client) resetSession(old uint64) {
	c.sessMu.Lock()
	if c.clientID == old {
		c.clientID, c.seq = 0, 0
		c.inflight = make(map[uint64]struct{})
	}
	c.sessMu.Unlock()
}

// begin allocates the next sequence number. ack is everything below the
// oldest request that is still in flight, which the server may forget.
func (c *Client) begin(id uint64) (seq, ack uint64) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	c.seq++
	seq = c.seq
	ack = seq - 1
	for s := range c.inflight {
		if s-1 < ack {
			ack = s - 1
		}
	}
	c.inflight[seq] = struct{}{}
	return
}

func (c *Client) end(seq uint64) {
	c.sessMu.Lock()
	delete(c.inflight, seq)
	c.sessMu.Unlock()
}

// write runs a session-protected mutation with transparent retries.
func (c *Client) write(ctx context.Context, fn func(ctx context.Context, cc *grpc.ClientConn, meta *kvpb.RequestMeta) error) error {
	for reopen := 0; ; reopen++ {
		id, err := c.ensureSession(ctx)
		if err != nil {
			return err
		}
		seq, ack := c.begin(id)
		meta := &kvpb.RequestMeta{ClientId: id, Seq: seq, AckSeq: ack}
		sent := 0
		err = c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) error {
			sent++
			return fn(ctx, cc, meta)
		})
		c.end(seq)
		if kvpb.Reason(err) == kvpb.ErrSessionExpired {
			c.resetSession(id)
			if sent <= 1 && reopen == 0 {
				continue // never reached the log, safe to replay on a fresh session
			}
			return ErrSessionLost
		}
		return err
	}
}

// ---- kv ----

type opConfig struct {
	end          []byte
	prefix       bool
	limit        uint64
	serializable bool
	keysOnly     bool
	countOnly    bool
	lease        uint64
	prevKV       bool
	startRev     uint64
	rev          uint64
}

type Option func(*opConfig)

func WithPrefix() Option                { return func(o *opConfig) { o.prefix = true } }
func WithRange(end []byte) Option       { return func(o *opConfig) { o.end = end } }
func WithFromKey() Option               { return func(o *opConfig) { o.end = []byte{0} } }
func WithLimit(n uint64) Option         { return func(o *opConfig) { o.limit = n } }
func WithSerializable() Option          { return func(o *opConfig) { o.serializable = true } }
func WithKeysOnly() Option              { return func(o *opConfig) { o.keysOnly = true } }
func WithCountOnly() Option             { return func(o *opConfig) { o.countOnly = true } }
func WithLease(id uint64) Option        { return func(o *opConfig) { o.lease = id } }
func WithPrevKV() Option                { return func(o *opConfig) { o.prevKV = true } }
func WithStartRevision(r uint64) Option { return func(o *opConfig) { o.startRev = r } }

// WithRev reads the store as it was at revision r.
func WithRev(r uint64) Option { return func(o *opConfig) { o.rev = r } }

func build(key []byte, opts []Option) (opConfig, []byte) {
	var o opConfig
	for _, f := range opts {
		f(&o)
	}
	end := o.end
	if o.prefix {
		end = prefixEnd(key)
	}
	return o, end
}

func prefixEnd(p []byte) []byte {
	e := append([]byte(nil), p...)
	for i := len(e) - 1; i >= 0; i-- {
		if e[i] < 0xff {
			e[i]++
			return e[:i+1]
		}
	}
	return []byte{0}
}

func (c *Client) Put(ctx context.Context, key, val string, opts ...Option) (*kvpb.PutResponse, error) {
	o, _ := build([]byte(key), opts)
	var resp *kvpb.PutResponse
	err := c.write(ctx, func(ctx context.Context, cc *grpc.ClientConn, meta *kvpb.RequestMeta) (err error) {
		resp, err = kvpb.NewKVClient(cc).Put(ctx, &kvpb.PutRequest{
			Meta: meta, Key: []byte(key), Value: []byte(val), Lease: o.lease, PrevKv: o.prevKV,
		})
		return
	})
	return resp, err
}

func (c *Client) Get(ctx context.Context, key string, opts ...Option) (*kvpb.RangeResponse, error) {
	o, end := build([]byte(key), opts)
	var resp *kvpb.RangeResponse
	err := c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) (err error) {
		resp, err = kvpb.NewKVClient(cc).Range(ctx, &kvpb.RangeRequest{
			Key: []byte(key), RangeEnd: end, Limit: o.limit, Serializable: o.serializable,
			KeysOnly: o.keysOnly, CountOnly: o.countOnly, Revision: o.rev,
		})
		return
	})
	return resp, err
}

// Compact drops history below rev. Reads at older revisions then fail with COMPACTED.
func (c *Client) Compact(ctx context.Context, rev uint64) error {
	return c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) error {
		_, err := kvpb.NewKVClient(cc).Compact(ctx, &kvpb.CompactRequest{Revision: rev})
		return err
	})
}

func (c *Client) Delete(ctx context.Context, key string, opts ...Option) (*kvpb.DeleteRangeResponse, error) {
	o, end := build([]byte(key), opts)
	var resp *kvpb.DeleteRangeResponse
	err := c.write(ctx, func(ctx context.Context, cc *grpc.ClientConn, meta *kvpb.RequestMeta) (err error) {
		resp, err = kvpb.NewKVClient(cc).DeleteRange(ctx, &kvpb.DeleteRangeRequest{
			Meta: meta, Key: []byte(key), RangeEnd: end, PrevKv: o.prevKV,
		})
		return
	})
	return resp, err
}

// Txn is a compare-and-swap builder: If(...).Then(...).Else(...).Commit(ctx).
type Txn struct {
	c    *Client
	cmps []*kvpb.Compare
	succ []*kvpb.RequestOp
	fail []*kvpb.RequestOp
}

func (c *Client) Txn() *Txn { return &Txn{c: c} }

func (t *Txn) If(cmps ...*kvpb.Compare) *Txn    { t.cmps = append(t.cmps, cmps...); return t }
func (t *Txn) Then(ops ...*kvpb.RequestOp) *Txn { t.succ = append(t.succ, ops...); return t }
func (t *Txn) Else(ops ...*kvpb.RequestOp) *Txn { t.fail = append(t.fail, ops...); return t }

func (t *Txn) Commit(ctx context.Context) (*kvpb.TxnResponse, error) {
	var resp *kvpb.TxnResponse
	err := t.c.write(ctx, func(ctx context.Context, cc *grpc.ClientConn, meta *kvpb.RequestMeta) (err error) {
		resp, err = kvpb.NewKVClient(cc).Txn(ctx, &kvpb.TxnRequest{
			Meta: meta, Compare: t.cmps, Success: t.succ, Failure: t.fail,
		})
		return
	})
	return resp, err
}

func CmpValue(key, val string) *kvpb.Compare {
	return &kvpb.Compare{
		Result: kvpb.Compare_EQUAL, Target: kvpb.Compare_VALUE, Key: []byte(key),
		TargetUnion: &kvpb.Compare_Value{Value: []byte(val)},
	}
}

func CmpModRevision(key string, result kvpb.Compare_Result, rev uint64) *kvpb.Compare {
	return &kvpb.Compare{
		Result: result, Target: kvpb.Compare_MOD, Key: []byte(key),
		TargetUnion: &kvpb.Compare_ModRevision{ModRevision: rev},
	}
}

func CmpCreateRevision(key string, result kvpb.Compare_Result, rev uint64) *kvpb.Compare {
	return &kvpb.Compare{
		Result: result, Target: kvpb.Compare_CREATE, Key: []byte(key),
		TargetUnion: &kvpb.Compare_CreateRevision{CreateRevision: rev},
	}
}

func OpPut(key, val string, opts ...Option) *kvpb.RequestOp {
	o, _ := build([]byte(key), opts)
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_Put{Put: &kvpb.PutRequest{
		Key: []byte(key), Value: []byte(val), Lease: o.lease, PrevKv: o.prevKV,
	}}}
}

func OpGet(key string, opts ...Option) *kvpb.RequestOp {
	o, end := build([]byte(key), opts)
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_Range{Range: &kvpb.RangeRequest{
		Key: []byte(key), RangeEnd: end, Limit: o.limit, KeysOnly: o.keysOnly,
	}}}
}

func OpDelete(key string, opts ...Option) *kvpb.RequestOp {
	o, end := build([]byte(key), opts)
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_DeleteRange{DeleteRange: &kvpb.DeleteRangeRequest{
		Key: []byte(key), RangeEnd: end, PrevKv: o.prevKV,
	}}}
}

// ---- cluster ----

// Status asks one specific endpoint, whatever its role.
func (c *Client) Status(ctx context.Context, endpoint string) (*kvpb.StatusResponse, error) {
	cc, err := c.conn(endpoint)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	return kvpb.NewClusterClient(cc).Status(ctx, &kvpb.StatusRequest{})
}

func (c *Client) Endpoints() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.cfg.Endpoints...)
}
