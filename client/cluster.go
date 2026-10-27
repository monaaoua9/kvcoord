package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

func (c *Client) Members(ctx context.Context) ([]*kvpb.Member, error) {
	var resp *kvpb.MemberListResponse
	err := c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) (err error) {
		resp, err = kvpb.NewClusterClient(cc).MemberList(ctx, &kvpb.MemberListRequest{})
		return
	})
	if err != nil {
		return nil, err
	}
	return resp.Members, nil
}

// MemberAdd brings a node into the cluster the safe way: it joins as a learner,
// receives the log, and is promoted to voter once it has caught up. Start the
// new node with -join first. Calling it again after a timeout resumes where the
// first call stopped.
func (c *Client) MemberAdd(ctx context.Context, id uint64, addr string) ([]*kvpb.Member, error) {
	if _, err := c.MemberAddLearner(ctx, id, addr); err != nil {
		ms, lerr := c.Members(ctx)
		if lerr != nil || kvpb.Reason(err) != kvpb.ErrInvalidArgument || !isLearner(ms, id) {
			return nil, err
		}
	}
	return c.MemberPromote(ctx, id)
}

func isLearner(ms []*kvpb.Member, id uint64) bool {
	for _, m := range ms {
		if m.Id == id && m.Learner {
			return true
		}
	}
	return false
}

// MemberAddLearner adds a non-voting member that only receives the log.
func (c *Client) MemberAddLearner(ctx context.Context, id uint64, addr string) ([]*kvpb.Member, error) {
	return c.memberChange(ctx, func(ctx context.Context, cc *grpc.ClientConn) (*kvpb.MemberListResponse, error) {
		return kvpb.NewClusterClient(cc).MemberAdd(ctx, &kvpb.MemberAddRequest{Id: id, Addr: addr, Learner: true})
	})
}

// MemberPromote makes a learner a voter, waiting while it is still catching up.
func (c *Client) MemberPromote(ctx context.Context, id uint64) ([]*kvpb.Member, error) {
	return c.memberChange(ctx, func(ctx context.Context, cc *grpc.ClientConn) (*kvpb.MemberListResponse, error) {
		return kvpb.NewClusterClient(cc).MemberPromote(ctx, &kvpb.MemberPromoteRequest{Id: id})
	})
}

func (c *Client) MemberRemove(ctx context.Context, id uint64) ([]*kvpb.Member, error) {
	return c.memberChange(ctx, func(ctx context.Context, cc *grpc.ClientConn) (*kvpb.MemberListResponse, error) {
		return kvpb.NewClusterClient(cc).MemberRemove(ctx, &kvpb.MemberRemoveRequest{Id: id})
	})
}

func (c *Client) memberChange(ctx context.Context, fn func(context.Context, *grpc.ClientConn) (*kvpb.MemberListResponse, error)) ([]*kvpb.Member, error) {
	for {
		var resp *kvpb.MemberListResponse
		err := c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) (err error) {
			resp, err = fn(ctx, cc)
			return
		})
		if r := kvpb.Reason(err); r == kvpb.ErrConfPending || r == kvpb.ErrLearnerLagging {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		return resp.Members, nil
	}
}

// TransferLeader asks the current leader to hand over to target (0 = best
// candidate). The call is routed to the leader like any write.
func (c *Client) TransferLeader(ctx context.Context, target uint64) error {
	return c.do(ctx, c.cfg.RequestTimeout, func(ctx context.Context, cc *grpc.ClientConn) error {
		_, err := kvpb.NewClusterClient(cc).TransferLeader(ctx, &kvpb.TransferLeaderRequest{Target: target})
		return err
	})
}

// MemberReplace swaps old for a new node in three committed steps: add the new
// one as a learner, promote it once it has caught up, then remove old. Each step
// keeps a majority of the previous configuration reachable, so a failure in the
// middle leaves a working cluster (with one extra voter, or one learner). Calling
// it again resumes from whatever state the cluster is in.
func (c *Client) MemberReplace(ctx context.Context, oldID, newID uint64, newAddr string) ([]*kvpb.Member, error) {
	ms, err := c.Members(ctx)
	if err != nil {
		return nil, err
	}
	has := func(id uint64) *kvpb.Member {
		for _, m := range ms {
			if m.Id == id {
				return m
			}
		}
		return nil
	}
	if has(oldID) == nil && has(newID) == nil {
		return nil, fmt.Errorf("client: node %d is not a member", oldID)
	}
	if m := has(newID); m == nil || m.Learner {
		if _, err := c.MemberAdd(ctx, newID, newAddr); err != nil {
			return nil, err
		}
	}
	if has(oldID) == nil {
		return c.Members(ctx)
	}
	return c.MemberRemove(ctx, oldID)
}
