package raft

// MemoryLog is an in-memory LogStore.
//
// It backs the whole safety campaign: thousands of schedules would be
// unrunnable at any useful rate against files, and the protocol's correctness
// does not depend on where entries are kept. A file-backed store arrives when
// a real node needs one.
type MemoryLog struct {
	// ents[0] is a sentinel carrying the index and term immediately before the
	// first real entry. Keeping it removes the special case that would
	// otherwise appear everywhere a predecessor is named.
	ents []Entry
}

// NewMemoryLog returns an empty log whose first real index will be 1.
func NewMemoryLog() *MemoryLog {
	return &MemoryLog{ents: []Entry{{Term: 0, Index: 0}}}
}

// NewMemoryLogWith returns a log preloaded with entries, for tests that need a
// member to start from a given history.
func NewMemoryLogWith(ents ...Entry) *MemoryLog {
	l := NewMemoryLog()
	if len(ents) > 0 {
		if err := l.Append(ents); err != nil {
			panic("raft: preloaded entries are not contiguous from index 1: " + err.Error())
		}
	}
	return l
}

func (l *MemoryLog) offset() Index { return l.ents[0].Index }

func (l *MemoryLog) FirstIndex() Index { return l.offset() + 1 }

func (l *MemoryLog) LastIndex() Index { return l.ents[len(l.ents)-1].Index }

func (l *MemoryLog) Term(i Index) (Term, error) {
	switch {
	case i < l.offset():
		return 0, ErrCompacted
	case i > l.LastIndex():
		return 0, ErrUnavailable
	}
	return l.ents[i-l.offset()].Term, nil
}

func (l *MemoryLog) Entries(lo, hi Index, maxBytes uint64) ([]Entry, error) {
	switch {
	case lo <= l.offset():
		return nil, ErrCompacted
	case hi > l.LastIndex()+1:
		return nil, ErrUnavailable
	case lo >= hi:
		return nil, nil
	}

	window := l.ents[lo-l.offset() : hi-l.offset()]

	// Always take the first entry regardless of size, then stop as soon as
	// another would exceed the budget.
	size := entrySize(window[0])
	take := 1
	for take < len(window) {
		next := size + entrySize(window[take])
		if next > maxBytes {
			break
		}
		size = next
		take++
	}

	out := make([]Entry, take)
	copy(out, window[:take])
	return out, nil
}

func (l *MemoryLog) Append(ents []Entry) error {
	if len(ents) == 0 {
		return nil
	}
	if ents[0].Index != l.LastIndex()+1 {
		return ErrOutOfOrder
	}
	for i := 1; i < len(ents); i++ {
		if ents[i].Index != ents[i-1].Index+1 {
			return ErrOutOfOrder
		}
		if ents[i].Term < ents[i-1].Term {
			// Terms never decrease along a log. A batch that says otherwise is
			// malformed, and accepting it would corrupt every later term
			// comparison.
			return ErrOutOfOrder
		}
	}
	l.ents = append(l.ents, ents...)
	return nil
}

func (l *MemoryLog) TruncateSuffix(from Index) error {
	switch {
	case from <= l.offset():
		// Removing the sentinel would lose the log's origin.
		return ErrCompacted
	case from > l.LastIndex()+1:
		return ErrUnavailable
	}
	l.ents = l.ents[:from-l.offset()]
	return nil
}
