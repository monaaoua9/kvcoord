package client

import (
	"context"
	"time"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

type WatchEvent struct {
	Events   []*kvpb.Event
	Revision uint64
	Err      error // terminal: the channel closes right after
}

// Watch streams changes of key (or a range, with the usual options). If the
// connection drops it reconnects, possibly to another node, and resumes from
// the last revision it delivered. It only gives up when the history it needs
// has been compacted, in which case the caller has to re-read and re-watch.
func (c *Client) Watch(ctx context.Context, key string, opts ...Option) <-chan WatchEvent {
	o, end := build([]byte(key), opts)
	out := make(chan WatchEvent, 64)
	go c.watchLoop(ctx, out, &kvpb.WatchCreateRequest{
		Key: []byte(key), RangeEnd: end, StartRevision: o.startRev, PrevKv: o.prevKV,
	})
	return out
}

func (c *Client) watchLoop(ctx context.Context, out chan<- WatchEvent, req *kvpb.WatchCreateRequest) {
	defer close(out)
	var last uint64
	backoff := 20 * time.Millisecond

	for ctx.Err() == nil {
		if last > 0 {
			req.StartRevision = last + 1
		}
		err := c.watchOnce(ctx, out, req, &last)
		if ctx.Err() != nil {
			return
		}
		if err == ErrWatchCompact {
			send(ctx, out, WatchEvent{Err: err})
			return
		}
		c.rotate()
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 500*time.Millisecond)
	}
}

func send(ctx context.Context, out chan<- WatchEvent, ev WatchEvent) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *Client) watchOnce(ctx context.Context, out chan<- WatchEvent, req *kvpb.WatchCreateRequest, last *uint64) error {
	cc, err := c.conn(c.endpoint())
	if err != nil {
		return err
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := kvpb.NewWatchClient(cc).Watch(sctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&kvpb.WatchRequest{Request: &kvpb.WatchRequest_Create{Create: req}}); err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case resp.Canceled && resp.CancelReason == kvpb.ErrCompacted:
			return ErrWatchCompact
		case resp.Canceled:
			// overflow, snapshot restore or server shutdown: just resume
			return errNoLeaderRetry
		case resp.Created:
			continue
		}
		if len(resp.Events) == 0 {
			continue
		}
		rev := resp.Events[0].Kv.ModRevision
		if !send(ctx, out, WatchEvent{Events: resp.Events, Revision: rev}) {
			return ctx.Err()
		}
		*last = rev
	}
}
