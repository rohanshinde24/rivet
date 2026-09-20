package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// sessionEntryBytes is the fixed encoded size of one session entry in a
// snapshot: client ID, high-water sequence, digest, result kind, existed flag,
// and the global sequence at which the result was applied.
const sessionEntryBytes = 16 + 8 + 32 + 1 + 1 + 8

// sessionEntry is the retained deduplication state for one client.
type sessionEntry struct {
	LastSequence    uint64
	Digest          [32]byte
	ResultKind      OpKind
	Existed         bool
	AppliedSequence uint64
}

// writeResult is the deterministic result of a PUT or DELETE.
type writeResult struct {
	AppliedSequence uint64
	Existed         bool
	Duplicate       bool
}

// admission is what the session rules decided about a candidate write.
type admission uint8

const (
	// admitExecute means the command is new and must be appended and applied.
	admitExecute admission = iota
	// admitDuplicate means the stored result is returned without appending.
	admitDuplicate
)

// stateMachine is the deterministic KV and session state. It is owned
// exclusively by the engine event loop (ADR-0005); nothing else mutates it.
type stateMachine struct {
	kv       map[string][]byte
	sessions map[ClientID]sessionEntry
	applied  uint64
}

func newStateMachine() *stateMachine {
	return &stateMachine{
		kv:       make(map[string][]byte),
		sessions: make(map[ClientID]sessionEntry),
	}
}

// get returns a copy of the value for key. The copy is what keeps a caller
// from aliasing engine-owned bytes (ADR-0007).
func (s *stateMachine) get(key []byte) ([]byte, bool) {
	v, ok := s.kv[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// admit applies the session rules to a candidate live write. It never mutates
// state. A returned admitDuplicate carries the stored result.
func (s *stateMachine) admit(c *command, maxSessions int) (admission, writeResult, error) {
	const op = "admit"

	entry, known := s.sessions[c.ClientID]
	if !known {
		if c.Sequence != 1 {
			return 0, writeResult{}, errorf(CodeSequenceGap, op,
				"new client must start at sequence 1, got %d", c.Sequence)
		}
		if len(s.sessions) >= maxSessions {
			return 0, writeResult{}, errorf(CodeResourceExhausted, op,
				"session table at capacity %d", maxSessions)
		}
		return admitExecute, writeResult{}, nil
	}

	switch {
	case c.Sequence == entry.LastSequence+1:
		return admitExecute, writeResult{}, nil

	case c.Sequence == entry.LastSequence:
		if c.digest() != entry.Digest {
			return 0, writeResult{}, errorf(CodeRequestIDReuse, op,
				"sequence %d already used with different content", c.Sequence)
		}
		return admitDuplicate, writeResult{
			AppliedSequence: entry.AppliedSequence,
			Existed:         entry.Existed,
			Duplicate:       true,
		}, nil

	case c.Sequence < entry.LastSequence:
		return 0, writeResult{}, errorf(CodeDuplicateResultExpired, op,
			"sequence %d is below retained high-water %d", c.Sequence, entry.LastSequence)

	default:
		return 0, writeResult{}, errorf(CodeSequenceGap, op,
			"sequence %d skips past %d", c.Sequence, entry.LastSequence+1)
	}
}

// apply executes a durable command at globalSeq. It is the single place where
// KV and session state change, and it behaves identically on the live path and
// during replay (INV-003-5).
//
// Every precondition failure here is an invariant violation rather than a
// rejected request: on the live path admit has already passed, and during
// replay the command was durable and must be applicable (INV-003-9).
func (s *stateMachine) apply(c *command, globalSeq uint64, maxKeys int) (writeResult, error) {
	const op = "apply"

	if globalSeq != s.applied+1 {
		return writeResult{}, errorf(CodeCorruption, op,
			"command sequence %d is not %d", globalSeq, s.applied+1)
	}

	entry, known := s.sessions[c.ClientID]
	switch {
	case !known && c.Sequence != 1:
		return writeResult{}, errorf(CodeCorruption, op,
			"durable command opens client at sequence %d", c.Sequence)
	case known && c.Sequence != entry.LastSequence+1:
		return writeResult{}, errorf(CodeCorruption, op,
			"durable command sequence %d is not %d", c.Sequence, entry.LastSequence+1)
	}

	var res writeResult
	switch c.Kind {
	case OpPut:
		if _, exists := s.kv[string(c.Key)]; !exists && len(s.kv) >= maxKeys {
			return writeResult{}, errorf(CodeCorruption, op,
				"key count %d is at the configured maximum", len(s.kv))
		}
		s.kv[string(c.Key)] = append([]byte(nil), c.Value...)
		res.Existed = true

	case OpDelete:
		_, existed := s.kv[string(c.Key)]
		if existed {
			delete(s.kv, string(c.Key))
		}
		res.Existed = existed

	default:
		return writeResult{}, errorf(CodeCorruption, op, "unknown operation kind %d", c.Kind)
	}

	s.applied = globalSeq
	res.AppliedSequence = globalSeq

	s.sessions[c.ClientID] = sessionEntry{
		LastSequence:    c.Sequence,
		Digest:          c.digest(),
		ResultKind:      c.Kind,
		Existed:         res.Existed,
		AppliedSequence: globalSeq,
	}
	return res, nil
}

// clone returns a deep, immutable-by-convention copy for a snapshot worker.
// The worker never touches live state (ADR-0004, ADR-0005).
func (s *stateMachine) clone() *stateMachine {
	out := &stateMachine{
		kv:       make(map[string][]byte, len(s.kv)),
		sessions: make(map[ClientID]sessionEntry, len(s.sessions)),
		applied:  s.applied,
	}
	for k, v := range s.kv {
		out.kv[k] = append([]byte(nil), v...)
	}
	for id, e := range s.sessions {
		out.sessions[id] = e
	}
	return out
}

// sortedKeys returns live keys in the lexicographic order snapshots use.
func (s *stateMachine) sortedKeys() []string {
	keys := make([]string, 0, len(s.kv))
	for k := range s.kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortedClients returns session identifiers in the order snapshots use.
func (s *stateMachine) sortedClients() []ClientID {
	ids := make([]ClientID, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		for b := range ids[i] {
			if ids[i][b] != ids[j][b] {
				return ids[i][b] < ids[j][b]
			}
		}
		return false
	})
	return ids
}

// digest is a deterministic fingerprint of the entire logical state. Two
// recoveries of the same durable bytes must produce the same value
// (INV-003-5); tests compare it rather than walking maps.
func (s *stateMachine) digest() [32]byte {
	var scratch [8]byte
	h := sha256.New()
	h.Write([]byte("rivet.storage.state.v1\x00"))

	binary.LittleEndian.PutUint64(scratch[:], s.applied)
	h.Write(scratch[:])

	binary.LittleEndian.PutUint64(scratch[:], uint64(len(s.kv)))
	h.Write(scratch[:])
	for _, k := range s.sortedKeys() {
		v := s.kv[k]
		binary.LittleEndian.PutUint64(scratch[:], uint64(len(k)))
		h.Write(scratch[:])
		h.Write([]byte(k))
		binary.LittleEndian.PutUint64(scratch[:], uint64(len(v)))
		h.Write(scratch[:])
		h.Write(v)
	}

	binary.LittleEndian.PutUint64(scratch[:], uint64(len(s.sessions)))
	h.Write(scratch[:])
	for _, id := range s.sortedClients() {
		e := s.sessions[id]
		h.Write(id[:])
		binary.LittleEndian.PutUint64(scratch[:], e.LastSequence)
		h.Write(scratch[:])
		h.Write(e.Digest[:])
		var flags [2]byte
		flags[0] = byte(e.ResultKind)
		if e.Existed {
			flags[1] = 1
		}
		h.Write(flags[:])
		binary.LittleEndian.PutUint64(scratch[:], e.AppliedSequence)
		h.Write(scratch[:])
	}

	var out [32]byte
	h.Sum(out[:0])
	return out
}
