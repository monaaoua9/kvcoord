package tlsutil

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/monaaoua9/kvcoord/api/kvpb"
)

type authority struct {
	dir  string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	n    int64
}

func newCA(t *testing.T) *authority {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	a := &authority{dir: t.TempDir(), cert: cert, key: key, n: 1}
	writePEM(t, filepath.Join(a.dir, "ca.pem"), "CERTIFICATE", der)
	return a
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

// issue signs a leaf for localhost and returns the cert and key file paths.
func (a *authority) issue(t *testing.T, name string, signer *authority) (string, string) {
	t.Helper()
	if signer == nil {
		signer = a
	}
	a.n++
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(a.n), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, signer.cert, &key.PublicKey, signer.key)
	if err != nil {
		t.Fatal(err)
	}
	kder, _ := x509.MarshalECPrivateKey(key)
	cp, kp := filepath.Join(a.dir, name+".pem"), filepath.Join(a.dir, name+".key")
	writePEM(t, cp, "CERTIFICATE", der)
	writePEM(t, kp, "EC PRIVATE KEY", kder)
	return cp, kp
}

// serve starts a server that only answers with Unimplemented, which is enough
// to tell a completed handshake from a refused one.
func serve(t *testing.T, ca string, a *authority) string {
	t.Helper()
	cert, key := a.issue(t, "server", nil)
	creds, err := Server(cert, key, ca)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(creds))
	kvpb.RegisterClusterServer(s, kvpb.UnimplementedClusterServer{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l.Addr().String()
}

func call(t *testing.T, addr string, creds grpc.DialOption) codes.Code {
	t.Helper()
	cc, err := grpc.NewClient(addr, creds)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = kvpb.NewClusterClient(cc).MemberList(ctx, &kvpb.MemberListRequest{})
	return status.Code(err)
}

func TestMutualTLS(t *testing.T) {
	ca := newCA(t)
	caPath := filepath.Join(ca.dir, "ca.pem")
	addr := serve(t, caPath, ca)

	good, key := ca.issue(t, "client", nil)
	cc, err := Client(good, key, caPath, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if got := call(t, addr, grpc.WithTransportCredentials(cc)); got != codes.Unimplemented {
		t.Fatalf("valid client: %v", got)
	}

	// a certificate from some other CA must be refused
	other := newCA(t)
	bad, bkey := other.issue(t, "intruder", nil)
	cc, err = Client(bad, bkey, caPath, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if got := call(t, addr, grpc.WithTransportCredentials(cc)); got != codes.Unavailable {
		t.Fatalf("client with a foreign certificate: %v", got)
	}

	// no client certificate at all
	cc, err = Client("", "", caPath, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if got := call(t, addr, grpc.WithTransportCredentials(cc)); got != codes.Unavailable {
		t.Fatalf("client without a certificate: %v", got)
	}
}

func TestClientChecksServerIdentity(t *testing.T) {
	ca := newCA(t)
	caPath := filepath.Join(ca.dir, "ca.pem")
	addr := serve(t, caPath, ca)
	cert, key := ca.issue(t, "client", nil)

	// wrong expected name: the server certificate is for localhost only
	cc, err := Client(cert, key, caPath, "not-the-server.example")
	if err != nil {
		t.Fatal(err)
	}
	if got := call(t, addr, grpc.WithTransportCredentials(cc)); got != codes.Unavailable {
		t.Fatalf("wrong server name: %v", got)
	}

	// a client that does not trust the CA
	other := newCA(t)
	cc, err = Client(cert, key, filepath.Join(other.dir, "ca.pem"), "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if got := call(t, addr, grpc.WithTransportCredentials(cc)); got != codes.Unavailable {
		t.Fatalf("untrusted CA: %v", got)
	}
}

func TestBadInputs(t *testing.T) {
	if _, err := Client("only-cert.pem", "", "", ""); err == nil {
		t.Fatal("cert without key should be rejected")
	}
	empty := filepath.Join(t.TempDir(), "empty.pem")
	os.WriteFile(empty, []byte("not pem"), 0o600)
	if _, err := Client("", "", empty, ""); err == nil {
		t.Fatal("a CA file without certificates should be rejected")
	}
}
