package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

var ErrCrossShard = errors.New("client: keys of this transaction live on different shards")

// Sharded spreads the keyspace over several independent kvcoord clusters. A key
// goes to shard fnv64(key) mod N, so the shard count is fixed for the life of the
// data: changing it means moving keys, which this client does not do.
//
// Each shard is a full Raft group with its own linearizability, revisions and
// sessions. Point operations keep every guarantee of the single-cluster client.
// Operations that span shards do not: range reads and deletes fan out and the
// result is not a snapshot of the whole keyspace, and a transaction must stay on
// one shard (ErrCrossShard otherwise). Revisions are per shard.
type Sharded struct {
	shards []*Client
}

// NewSharded dials one cluster per entry of shards (its endpoints).
func NewSharded(shards [][]string, base Config) (*Sharded, error) {
	if len(shards) == 0 {
		return nil, errors.New("client: no shards")
	}
	s := &Sharded{}
	for i, eps := range shards {
		cfg := base
		cfg.Endpoints = eps
		c, err := New(cfg)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
		s.shards = append(s.shards, c)
	}
	return s, nil
}

func (s *Sharded) Close() {
	for _, c := range s.shards {
		c.Close()
	}
}

func (s *Sharded) Shards() int { return len(s.shards) }

// ShardOf says which shard (0-based, in the order given to NewSharded) owns key.
func (s *Sharded) ShardOf(key string) int { return s.index(key) }

func (s *Sharded) index(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(len(s.shards)))
}

// For returns the cluster that owns key, for things the wrapper does not cover:
// open a Session and a Mutex on For(lockName), and both live on one shard.
func (s *Sharded) For(key string) *Client { return s.shards[s.index(key)] }

func (s *Sharded) Put(ctx context.Context, key, val string, opts ...Option) (*kvpb.PutResponse, error) {
	return s.For(key).Put(ctx, key, val, opts...)
}

// isRange reports whether the options turn a get or delete into a range.
func isRange(key string, opts []Option) bool {
	_, end := build([]byte(key), opts)
	return len(end) > 0
}

func (s *Sharded) Get(ctx context.Context, key string, opts ...Option) (*kvpb.RangeResponse, error) {
	if !isRange(key, opts) {
		return s.For(key).Get(ctx, key, opts...)
	}
	o, _ := build([]byte(key), opts)
	resps := make([]*kvpb.RangeResponse, len(s.shards))
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i, c := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resps[i], errs[i] = c.Get(ctx, key, opts...)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	out := &kvpb.RangeResponse{}
	for _, r := range resps {
		out.Kvs = append(out.Kvs, r.Kvs...)
		out.Count += r.Count
		out.More = out.More || r.More
	}
	slices.SortFunc(out.Kvs, func(a, b *kvpb.KeyValue) int { return bytes.Compare(a.Key, b.Key) })
	if o.limit > 0 && uint64(len(out.Kvs)) > o.limit {
		out.Kvs = out.Kvs[:o.limit]
		out.More = true
	}
	if !o.countOnly {
		out.Count = uint64(len(out.Kvs))
	}
	return out, nil
}

func (s *Sharded) Delete(ctx context.Context, key string, opts ...Option) (*kvpb.DeleteRangeResponse, error) {
	if !isRange(key, opts) {
		return s.For(key).Delete(ctx, key, opts...)
	}
	resps := make([]*kvpb.DeleteRangeResponse, len(s.shards))
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i, c := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resps[i], errs[i] = c.Delete(ctx, key, opts...)
		}()
	}
	wg.Wait()
	// some shards may have deleted already; the caller learns that from the error
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	out := &kvpb.DeleteRangeResponse{}
	for _, r := range resps {
		out.Deleted += r.Deleted
		out.PrevKvs = append(out.PrevKvs, r.PrevKvs...)
	}
	return out, nil
}

// ShardedTxn is Txn that checks every key it touches maps to the same shard.
type ShardedTxn struct {
	s    *Sharded
	cmps []*kvpb.Compare
	succ []*kvpb.RequestOp
	fail []*kvpb.RequestOp
}

func (s *Sharded) Txn() *ShardedTxn { return &ShardedTxn{s: s} }

func (t *ShardedTxn) If(c ...*kvpb.Compare) *ShardedTxn     { t.cmps = append(t.cmps, c...); return t }
func (t *ShardedTxn) Then(o ...*kvpb.RequestOp) *ShardedTxn { t.succ = append(t.succ, o...); return t }
func (t *ShardedTxn) Else(o ...*kvpb.RequestOp) *ShardedTxn { t.fail = append(t.fail, o...); return t }

func (t *ShardedTxn) Commit(ctx context.Context) (*kvpb.TxnResponse, error) {
	shard := -1
	see := func(key []byte, rng bool) error {
		if rng {
			return fmt.Errorf("%w: range operations cannot be part of a sharded transaction", ErrCrossShard)
		}
		i := t.s.index(string(key))
		if shard >= 0 && i != shard {
			return ErrCrossShard
		}
		shard = i
		return nil
	}
	for _, c := range t.cmps {
		if err := see(c.Key, false); err != nil {
			return nil, err
		}
	}
	for _, ops := range [][]*kvpb.RequestOp{t.succ, t.fail} {
		for _, op := range ops {
			var err error
			switch r := op.Request.(type) {
			case *kvpb.RequestOp_Put:
				err = see(r.Put.Key, false)
			case *kvpb.RequestOp_Range:
				err = see(r.Range.Key, len(r.Range.RangeEnd) > 0)
			case *kvpb.RequestOp_DeleteRange:
				err = see(r.DeleteRange.Key, len(r.DeleteRange.RangeEnd) > 0)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	if shard < 0 {
		return nil, errors.New("client: empty transaction")
	}
	return t.s.shards[shard].Txn().If(t.cmps...).Then(t.succ...).Else(t.fail...).Commit(ctx)
}

// Watch merges the streams of every shard (a single key only talks to its own).
// Events are ordered within a shard; across shards there is no global order.
func (s *Sharded) Watch(ctx context.Context, key string, opts ...Option) <-chan WatchEvent {
	if !isRange(key, opts) {
		return s.For(key).Watch(ctx, key, opts...)
	}
	out := make(chan WatchEvent, 64)
	var wg sync.WaitGroup
	for _, c := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range c.Watch(ctx, key, opts...) {
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(out) }()
	return out
}
