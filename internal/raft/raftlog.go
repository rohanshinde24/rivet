package raft

// raftLog is the core's view of the replicated log.
//
// It exists because the driver, not the core, writes to the log store: the
// core stages entries in a Ready batch and only learns they are stored when
// Advance says so. Until then those entries are real to the protocol — they
// must be counted in the last index, compared against a candidate's log, and
// sent to followers — but absent from the store. This type holds that
// distinction in one place instead of scattering it through the protocol.
type raftLog struct {
	store LogStore

	// unstable holds entries staged for the driver but not yet in the store.
	// Its first index may be at or below the store's last index, which is how
	// a divergent suffix is replaced: the driver truncates at that index
	// before appending.
	unstable []Entry

	committed Index
	applied   Index
}

func newRaftLog(store LogStore) *raftLog {
	return &raftLog{store: store}
}

func (l *raftLog) lastIndex() Index {
	if n := len(l.unstable); n > 0 {
		return l.unstable[n-1].Index
	}
	return l.store.LastIndex()
}

// firstUnstable is the lowest index held only in memory.
func (l *raftLog) firstUnstable() Index {
	if len(l.unstable) == 0 {
		return l.store.LastIndex() + 1
	}
	return l.unstable[0].Index
}

func (l *raftLog) term(i Index) (Term, error) {
	if n := len(l.unstable); n > 0 && i >= l.unstable[0].Index {
		if i > l.unstable[n-1].Index {
			return 0, ErrUnavailable
		}
		return l.unstable[i-l.unstable[0].Index].Term, nil
	}
	return l.store.Term(i)
}

func (l *raftLog) lastTerm() Term {
	t, _ := l.term(l.lastIndex())
	return t
}

// entries returns [lo, hi), drawing from the store and the staged entries as
// needed. Like the store, it always returns at least one entry when lo is in
// range, so that a single large command can never become unsendable.
func (l *raftLog) entries(lo, hi Index, maxBytes uint64) ([]Entry, error) {
	if lo >= hi {
		return nil, nil
	}
	if hi > l.lastIndex()+1 {
		return nil, ErrUnavailable
	}

	split := l.firstUnstable()
	var out []Entry

	if lo < split {
		end := min(hi, split)
		stored, err := l.store.Entries(lo, end, maxBytes)
		if err != nil {
			return nil, err
		}
		out = stored
		// A short read means the byte budget was reached inside the store.
		if Index(len(stored)) < end-lo {
			return out, nil
		}
	}

	if hi > split {
		start := max(lo, split)
		staged := l.unstable[start-l.unstable[0].Index : hi-l.unstable[0].Index]
		out = appendWithinBudget(out, staged, maxBytes)
	}
	return out, nil
}

// appendWithinBudget adds entries while the total stays within maxBytes, and
// always keeps at least one entry overall.
func appendWithinBudget(out []Entry, add []Entry, maxBytes uint64) []Entry {
	var size uint64
	for _, e := range out {
		size += entrySize(e)
	}
	for _, e := range add {
		if len(out) > 0 && size+entrySize(e) > maxBytes {
			break
		}
		out = append(out, e)
		size += entrySize(e)
	}
	return out
}

// append stages entries that continue the log. It is the leader's path for
// its own proposals.
func (l *raftLog) append(ents ...Entry) {
	l.unstable = append(l.unstable, ents...)
}

// truncateAndAppend reconciles entries received from a leader.
//
// A prefix that already matches is skipped rather than rewritten, because a
// duplicated or retried append is ordinary traffic and must not disturb the
// log. Only from the first genuine conflict is anything replaced.
// It reports the index from which the log was replaced, or zero when the
// entries were already present, so the caller can say so.
func (l *raftLog) truncateAndAppend(ents []Entry) (replacedFrom Index, appended int) {
	if len(ents) == 0 {
		return 0, 0
	}

	i := 0
	for ; i < len(ents); i++ {
		t, err := l.term(ents[i].Index)
		if err != nil || t != ents[i].Term {
			break
		}
	}
	if i == len(ents) {
		return 0, 0
	}

	from := ents[i].Index
	replaced := Index(0)
	if from <= l.lastIndex() {
		replaced = from
	}
	l.truncateFrom(from)
	l.unstable = append(l.unstable, ents[i:]...)
	return replaced, len(ents) - i
}

// truncateFrom drops staged entries at or after idx. Entries already in the
// store are not removed here: staging a replacement that begins at idx is
// what tells the driver to truncate the store before appending.
func (l *raftLog) truncateFrom(idx Index) {
	if len(l.unstable) == 0 {
		return
	}
	first := l.unstable[0].Index
	if idx <= first {
		l.unstable = nil
		return
	}
	l.unstable = l.unstable[:idx-first]
}

// stableTo records that the driver has written everything through upto.
func (l *raftLog) stableTo(upto Index) {
	if len(l.unstable) == 0 {
		return
	}
	first := l.unstable[0].Index
	if upto < first {
		return
	}
	n := int(upto-first) + 1
	if n >= len(l.unstable) {
		l.unstable = nil
		return
	}
	l.unstable = l.unstable[n:]
}

func (l *raftLog) commitTo(i Index) {
	if i > l.committed && i <= l.lastIndex() {
		l.committed = i
	}
}

// nextCommitted returns entries that are committed but not yet handed to the
// driver for application.
func (l *raftLog) nextCommitted(maxBytes uint64) []Entry {
	if l.applied >= l.committed {
		return nil
	}
	ents, err := l.entries(l.applied+1, l.committed+1, maxBytes)
	if err != nil {
		return nil
	}
	return ents
}

func (l *raftLog) appliedTo(i Index) {
	if i > l.applied {
		l.applied = i
	}
}

// ApplyEntries writes a Ready batch's entries to a log store, replacing any
// divergent suffix first.
//
// Drivers should use this rather than reimplementing the rule. Staged entries
// may legitimately begin at or below the store's last index, and appending
// them without truncating first would either fail or, worse, leave two
// histories interleaved.
func ApplyEntries(store LogStore, ents []Entry) error {
	if len(ents) == 0 {
		return nil
	}
	if first := ents[0].Index; first <= store.LastIndex() {
		if err := store.TruncateSuffix(first); err != nil {
			return err
		}
	}
	return store.Append(ents)
}
