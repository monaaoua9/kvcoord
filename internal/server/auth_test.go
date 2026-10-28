package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseTokens(t *testing.T) {
	ok, err := parseTokens([]string{
		"# comment", "",
		"aaaaaaaaaaaaaaaa read", "bbbbbbbbbbbbbbbb write # trailing", "cccccccccccccccc admin",
	})
	if err != nil || len(ok) != 3 || ok["bbbbbbbbbbbbbbbb"] != roleWrite {
		t.Fatalf("%v %v", ok, err)
	}
	for name, in := range map[string][]string{
		"short token":  {"short read"},
		"unknown role": {"aaaaaaaaaaaaaaaa root"},
		"missing role": {"aaaaaaaaaaaaaaaa"},
		"extra field":  {"aaaaaaaaaaaaaaaa read now"},
		"duplicate":    {"aaaaaaaaaaaaaaaa read", "aaaaaaaaaaaaaaaa admin"},
	} {
		if _, err := parseTokens(in); err == nil {
			t.Errorf("%s should be rejected", name)
		}
	}
}

func TestLoadTokenFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "tokens")
	os.WriteFile(p, []byte("aaaaaaaaaaaaaaaa admin\nbbbbbbbbbbbbbbbb peer\n"), 0o600)
	m, err := LoadTokenFile(p)
	if err != nil || m["aaaaaaaaaaaaaaaa"] != "admin" || m["bbbbbbbbbbbbbbbb"] != "peer" {
		t.Fatalf("%v %v", m, err)
	}
	if _, err := LoadTokenFile(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("a missing file must be an error")
	}
}

func TestEveryRPCHasARole(t *testing.T) {
	// a new RPC silently becoming admin-only would be fine, one silently becoming
	// open is not; this just documents what the table covers
	for m, r := range methodRole {
		if !strings.HasPrefix(m, "/kvcoord.v1.") || r == 0 {
			t.Errorf("bad entry %s", m)
		}
	}
}
