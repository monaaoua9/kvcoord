package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

type role int

const (
	roleRead role = iota + 1
	roleWrite
	roleAdmin
	rolePeer // node-to-node traffic only
)

var roleNames = map[string]role{"read": roleRead, "write": roleWrite, "admin": roleAdmin, "peer": rolePeer}

// methodRole is the least role that may call a method; read < write < admin.
// Anything not listed needs admin, so a new RPC is closed until someone decides.
var methodRole = map[string]role{
	"/kvcoord.v1.KV/Range":             roleRead,
	"/kvcoord.v1.KV/OpenSession":       roleWrite,
	"/kvcoord.v1.KV/Put":               roleWrite,
	"/kvcoord.v1.KV/DeleteRange":       roleWrite,
	"/kvcoord.v1.KV/Txn":               roleWrite,
	"/kvcoord.v1.KV/Compact":           roleAdmin,
	"/kvcoord.v1.Watch/Watch":          roleRead,
	"/kvcoord.v1.Lease/Grant":          roleWrite,
	"/kvcoord.v1.Lease/Revoke":         roleWrite,
	"/kvcoord.v1.Lease/KeepAlive":      roleWrite,
	"/kvcoord.v1.Lease/TimeToLive":     roleRead,
	"/kvcoord.v1.Lock/Lock":            roleWrite,
	"/kvcoord.v1.Lock/Unlock":          roleWrite,
	"/kvcoord.v1.Cluster/Status":       roleRead,
	"/kvcoord.v1.Cluster/MemberList":   roleRead,
	"/kvcoord.v1.RaftTransport/Stream": rolePeer,
}

type authn struct {
	tokens map[string]role
}

// parseTokens reads "token role" pairs, one per line; # starts a comment.
func parseTokens(lines []string) (map[string]role, error) {
	out := make(map[string]role)
	for i, ln := range lines {
		if j := strings.IndexByte(ln, '#'); j >= 0 {
			ln = ln[:j]
		}
		f := strings.Fields(ln)
		if len(f) == 0 {
			continue
		}
		if len(f) != 2 {
			return nil, fmt.Errorf("auth: line %d: want \"token role\"", i+1)
		}
		r, ok := roleNames[f[1]]
		if !ok {
			return nil, fmt.Errorf("auth: line %d: unknown role %q", i+1, f[1])
		}
		if len(f[0]) < 16 {
			return nil, fmt.Errorf("auth: line %d: token is shorter than 16 characters", i+1)
		}
		if _, dup := out[f[0]]; dup {
			return nil, fmt.Errorf("auth: line %d: duplicate token", i+1)
		}
		out[f[0]] = r
	}
	return out, nil
}

// LoadTokenFile reads a token file in the format parseTokens accepts.
func LoadTokenFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	rs, err := parseTokens(lines)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rs))
	for t, r := range rs {
		for name, v := range roleNames {
			if v == r {
				out[t] = name
			}
		}
	}
	return out, nil
}

func newAuthn(tokens map[string]string) (*authn, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	a := &authn{tokens: make(map[string]role, len(tokens))}
	for t, name := range tokens {
		r, ok := roleNames[name]
		if !ok {
			return nil, fmt.Errorf("auth: unknown role %q", name)
		}
		if len(t) < 16 {
			return nil, fmt.Errorf("auth: tokens must be at least 16 characters")
		}
		a.tokens[t] = r
	}
	return a, nil
}

// lookup compares against every token so the time taken does not depend on how
// much of a guess was right.
func (a *authn) lookup(tok string) (role, bool) {
	var found role
	for t, r := range a.tokens {
		if subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
			found = r
		}
	}
	return found, found != 0
}

func (a *authn) check(ctx context.Context, method string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	var tok string
	if v := md.Get("authorization"); len(v) > 0 {
		tok = strings.TrimPrefix(v[0], "Bearer ")
	}
	have, ok := a.lookup(tok)
	if !ok {
		return kvpb.NewError(codes.Unauthenticated, kvpb.ErrUnauthenticated, "missing or unknown token", nil)
	}
	need, known := methodRole[method]
	if !known {
		need = roleAdmin
	}
	switch {
	case need == rolePeer:
		if have != rolePeer {
			return kvpb.NewError(codes.PermissionDenied, kvpb.ErrPermissionDenied, "node-to-node traffic needs a peer token", nil)
		}
	case have == rolePeer || have < need:
		return kvpb.NewError(codes.PermissionDenied, kvpb.ErrPermissionDenied, "this token may not call "+method, nil)
	}
	return nil
}

func (a *authn) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := a.check(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (a *authn) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if err := a.check(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return h(srv, ss)
}
