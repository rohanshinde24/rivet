package raft

import "errors"

// Errors a log store may return. They are sentinel values because the core
// branches on them: an unavailable index behind a follower is routine, while
// a compacted one means the follower needs a snapshot.
var (
	// ErrCompacted means the index is older than the first entry retained.
	ErrCompacted = errors.New("raft: log index is compacted")

	// ErrUnavailable means the index is beyond the last entry held.
	ErrUnavailable = errors.New("raft: log index is unavailable")

	// ErrOutOfOrder means an append did not continue from the last index.
	ErrOutOfOrder = errors.New("raft: append is not contiguous with the log")
)

// LogStore holds the replicated log.
//
// Two of these operations are the reason the Raft log cannot simply reuse the
// storage engine's write-ahead log. TruncateSuffix deliberately discards
// valid, checksummed entries whenever a follower's log diverges, which is the
// exact operation that log's tail policy exists to forbid. Entries re-reads
// arbitrary ranges continuously to catch followers up, where the storage log
// is write-only after recovery and reads back once, at startup.
//
// What the two share is the codec and the discipline, not this interface.
type LogStore interface {
	// FirstIndex is the oldest index still retained. It is 1 for a log that
	// has never been compacted.
	FirstIndex() Index

	// LastIndex is the newest index held, or 0 for an empty log.
	LastIndex() Index

	// Term returns the term of the entry at i. Index 0 is the sentinel that
	// precedes the log and has term 0, so a first append can name it as its
	// predecessor without a special case.
	Term(i Index) (Term, error)

	// Entries returns entries in [lo, hi), stopping early once maxBytes is
	// reached. It always returns at least one entry when lo is in range,
	// whatever its size: a single command may legitimately approach the
	// message limit on its own, and refusing to return it would make that
	// command permanently unreplicable.
	Entries(lo, hi Index, maxBytes uint64) ([]Entry, error)

	// Append adds entries that continue from LastIndex. A caller replacing a
	// divergent suffix truncates first, then appends.
	Append(ents []Entry) error

	// TruncateSuffix removes every entry at or after from. This is a normal
	// operation, not damage recovery: a leader uses it to overwrite a
	// follower's divergent tail.
	TruncateSuffix(from Index) error
}

// entryOverhead approximates the term and index accompanying an entry's
// payload, so that a size limit reflects what a message actually carries.
const entryOverhead = 16

func entrySize(e Entry) uint64 {
	return uint64(len(e.Data)) + entryOverhead
}
