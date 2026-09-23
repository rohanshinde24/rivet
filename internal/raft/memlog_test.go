package raft

import (
	"bytes"
	"errors"
	"testing"
)

func entries(spec ...[2]uint64) []Entry {
	out := make([]Entry, 0, len(spec))
	for _, s := range spec {
		out = append(out, Entry{Index: Index(s[0]), Term: Term(s[1])})
	}
	return out
}

func TestEmptyLog(t *testing.T) {
	l := NewMemoryLog()

	if got := l.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex %d, want 1", got)
	}
	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex %d, want 0", got)
	}

	// The sentinel must be addressable, so that a first append can name index
	// 0 as its predecessor without a special case.
	if term, err := l.Term(0); err != nil || term != 0 {
		t.Fatalf("Term(0) = (%d, %v), want (0, nil)", term, err)
	}
	if _, err := l.Term(1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(1) = %v, want ErrUnavailable", err)
	}
}

func TestAppendAndTermLookup(t *testing.T) {
	l := NewMemoryLog()
	if err := l.Append(entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2})); err != nil {
		t.Fatal(err)
	}

	if got := l.LastIndex(); got != 3 {
		t.Fatalf("LastIndex %d, want 3", got)
	}
	for _, tc := range []struct {
		index Index
		term  Term
	}{{0, 0}, {1, 1}, {2, 1}, {3, 2}} {
		got, err := l.Term(tc.index)
		if err != nil || got != tc.term {
			t.Fatalf("Term(%d) = (%d, %v), want (%d, nil)", tc.index, got, err, tc.term)
		}
	}
	if _, err := l.Term(4); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Term(4) = %v, want ErrUnavailable", err)
	}
}

func TestAppendRejectsMalformedBatches(t *testing.T) {
	cases := map[string][]Entry{
		"gap from the end": entries([2]uint64{2, 1}),
		"starts too low":   entries([2]uint64{0, 1}),
		"internal gap":     entries([2]uint64{1, 1}, [2]uint64{3, 1}),
		"decreasing term":  entries([2]uint64{1, 5}, [2]uint64{2, 4}),
		"repeated index":   entries([2]uint64{1, 1}, [2]uint64{1, 1}),
	}
	for name, ents := range cases {
		t.Run(name, func(t *testing.T) {
			l := NewMemoryLog()
			if err := l.Append(ents); !errors.Is(err, ErrOutOfOrder) {
				t.Fatalf("Append accepted %s: %v", name, err)
			}
		})
	}

	// An empty append is a no-op rather than an error, because a heartbeat
	// carries no entries.
	l := NewMemoryLog()
	if err := l.Append(nil); err != nil {
		t.Fatalf("empty append: %v", err)
	}
}

func TestEntriesRange(t *testing.T) {
	l := NewMemoryLog()
	if err := l.Append(entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2}, [2]uint64{4, 2})); err != nil {
		t.Fatal(err)
	}

	got, err := l.Entries(2, 4, DefaultMaxBytesPerMsg)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Index != 2 || got[1].Index != 3 {
		t.Fatalf("Entries(2,4) = %v", got)
	}

	if got, err := l.Entries(3, 3, DefaultMaxBytesPerMsg); err != nil || len(got) != 0 {
		t.Fatalf("empty range = (%v, %v)", got, err)
	}
	if _, err := l.Entries(0, 2, DefaultMaxBytesPerMsg); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Entries from the sentinel = %v, want ErrCompacted", err)
	}
	if _, err := l.Entries(1, 6, DefaultMaxBytesPerMsg); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Entries past the end = %v, want ErrUnavailable", err)
	}
	// The half-open bound must allow reading exactly to the end.
	if got, err := l.Entries(1, 5, DefaultMaxBytesPerMsg); err != nil || len(got) != 4 {
		t.Fatalf("Entries(1,5) = (%d entries, %v)", len(got), err)
	}
}

// A returned batch must not alias the log, or a caller mutating a message
// would rewrite history.
func TestEntriesReturnsACopy(t *testing.T) {
	l := NewMemoryLog()
	if err := l.Append([]Entry{{Index: 1, Term: 1, Data: []byte("original")}}); err != nil {
		t.Fatal(err)
	}

	got, err := l.Entries(1, 2, DefaultMaxBytesPerMsg)
	if err != nil {
		t.Fatal(err)
	}
	got[0].Term = 99

	if term, _ := l.Term(1); term != 1 {
		t.Fatalf("mutating the returned batch changed the log: term is now %d", term)
	}
}

func TestEntriesRespectsByteBudget(t *testing.T) {
	l := NewMemoryLog()
	var ents []Entry
	for i := 1; i <= 5; i++ {
		ents = append(ents, Entry{Index: Index(i), Term: 1, Data: bytes.Repeat([]byte("x"), 100)})
	}
	if err := l.Append(ents); err != nil {
		t.Fatal(err)
	}

	// Two entries fit in 240 bytes at 116 each; a third would not.
	got, err := l.Entries(1, 6, 240)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries under a 240 byte budget, want 2", len(got))
	}
}

// A single command may legitimately approach the message limit. Refusing to
// return it would make that command permanently unreplicable, which is a
// liveness failure a suite using small values would never notice.
func TestEntriesAlwaysReturnsAtLeastOne(t *testing.T) {
	l := NewMemoryLog()
	huge := bytes.Repeat([]byte("x"), 1<<20)
	if err := l.Append([]Entry{{Index: 1, Term: 1, Data: huge}}); err != nil {
		t.Fatal(err)
	}

	got, err := l.Entries(1, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries under a 1 byte budget, want 1", len(got))
	}
	if len(got[0].Data) != len(huge) {
		t.Fatal("the oversized entry was truncated rather than returned whole")
	}
}

func TestTruncateSuffix(t *testing.T) {
	build := func() *MemoryLog {
		l := NewMemoryLog()
		if err := l.Append(entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2})); err != nil {
			t.Fatal(err)
		}
		return l
	}

	l := build()
	if err := l.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if got := l.LastIndex(); got != 1 {
		t.Fatalf("LastIndex after truncation %d, want 1", got)
	}
	if _, err := l.Term(2); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("truncated index still readable: %v", err)
	}

	// The whole point of truncation is that a different suffix can follow.
	if err := l.Append(entries([2]uint64{2, 7})); err != nil {
		t.Fatalf("append after truncation: %v", err)
	}
	if term, _ := l.Term(2); term != 7 {
		t.Fatalf("overwritten entry has term %d, want 7", term)
	}

	l = build()
	if err := l.TruncateSuffix(4); err != nil {
		t.Fatalf("truncating to one past the end should be a no-op: %v", err)
	}
	if got := l.LastIndex(); got != 3 {
		t.Fatalf("no-op truncation changed LastIndex to %d", got)
	}

	if err := build().TruncateSuffix(0); !errors.Is(err, ErrCompacted) {
		t.Fatalf("truncating the sentinel = %v, want ErrCompacted", err)
	}
	if err := build().TruncateSuffix(5); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("truncating past the end = %v, want ErrUnavailable", err)
	}
}

func TestNewMemoryLogWith(t *testing.T) {
	l := NewMemoryLogWith(entries([2]uint64{1, 1}, [2]uint64{2, 3})...)
	if l.LastIndex() != 2 {
		t.Fatalf("LastIndex %d, want 2", l.LastIndex())
	}
	if term, _ := l.Term(2); term != 3 {
		t.Fatalf("Term(2) %d, want 3", term)
	}
}

// MemoryLog must satisfy the interface the core is written against.
var _ LogStore = (*MemoryLog)(nil)
