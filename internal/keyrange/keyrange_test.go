package keyrange

import "testing"

func TestContains(t *testing.T) {
	cases := []struct {
		start, end, key string
		want            bool
	}{
		{"a", "", "a", true},
		{"a", "", "ab", false},
		{"a", "c", "b", true},
		{"a", "c", "c", false},
		{"a", "c", "0", false},
		{"b", "\x00", "zzz", true},
		{"b", "\x00", "a", false},
	}
	for _, c := range cases {
		if got := Contains([]byte(c.start), []byte(c.end), []byte(c.key)); got != c.want {
			t.Errorf("Contains(%q,%q,%q) = %v", c.start, c.end, c.key, got)
		}
	}
}

func TestPrefixEnd(t *testing.T) {
	if got := string(PrefixEnd([]byte("abc"))); got != "abd" {
		t.Fatalf("got %q", got)
	}
	if got := string(PrefixEnd([]byte("ab\xff"))); got != "ac" {
		t.Fatalf("got %q", got)
	}
	if got := PrefixEnd([]byte{0xff, 0xff}); len(got) != 1 || got[0] != 0 {
		t.Fatalf("all-0xff prefix should scan to the end, got %v", got)
	}
}
