// Package raft implements the consensus protocol as a deterministic value.
//
// The core reads no clock, performs no I/O, and starts no goroutine. Time
// enters only as a tick and randomness only from an explicitly seeded source,
// which is what lets any failing schedule be replayed from its seed alone.
// Everything that touches a disk or a network lives in the driver.
package raft

import "fmt"

// Term is a Raft term. It never decreases for a given member.
type Term uint64

// Index is a position in the replicated log. The first real entry is at 1;
// index 0 is the sentinel meaning "before the beginning".
type Index uint64

// NodeID identifies a member of a group. Identity is fixed for the lifetime
// of a group, since membership change is not part of this milestone.
type NodeID uint64

// Entry is one replicated log record.
type Entry struct {
	Term  Term
	Index Index
	Data  []byte
}

// Role is a member's current role in its term.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// None is the absence of a node: no vote cast, no leader known.
const None NodeID = 0

// HardState is the state that must be durable before any message depending on
// it is sent.
//
// The commit index is deliberately absent. It is cheap to relearn from the
// next heartbeat, and persisting it would add a durable field that can
// disagree with the log after a partial write, which is a corruption class
// bought for a saving a single heartbeat already erases.
type HardState struct {
	Term Term
	Vote NodeID
}

// MessageType identifies a protocol message.
type MessageType uint8

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResp
	MsgAppend
	MsgAppendResp
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResp:
		return "RequestVoteResp"
	case MsgAppend:
		return "Append"
	case MsgAppendResp:
		return "AppendResp"
	default:
		return "Unknown"
	}
}

// Message is a protocol message. It carries only what the protocol needs: no
// deadline, address, trace identifier, or clock reading is ever a field here,
// because anything in this struct can end up deciding an election.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// RequestVote: the candidate's last log position, used to decide whether
	// its log is at least as up to date as the voter's.
	LastLogIndex Index
	LastLogTerm  Term

	// Append: the position the follower must already agree on, the entries
	// that follow it, and how far the leader has committed.
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index

	// Responses.
	Reject bool

	// MatchIndex is the highest index the sender now holds, set on an accepted
	// append response.
	MatchIndex Index

	// RejectHint and RejectTerm let a leader back up by more than one index
	// per round trip after a rejected append. Without them a follower that is
	// far behind costs one round trip per missing entry.
	RejectHint Index
	RejectTerm Term
}

func (m Message) String() string {
	return fmt.Sprintf("%s %d->%d term=%d", m.Type, m.From, m.To, m.Term)
}

// EventType identifies an input to the core.
type EventType uint8

const (
	// EventTick advances logical time by one unit.
	EventTick EventType = iota
	// EventPropose offers a command to this member.
	EventPropose
	// EventMessage delivers a protocol message.
	EventMessage
	// EventCampaign forces an election immediately. It exists so a test can
	// drive an election without waiting out a timeout, and has no production
	// caller.
	EventCampaign
)

// Event is one input to the core.
type Event struct {
	Type    EventType
	Message Message
	Data    []byte
}

// StateChange reports a role, term, or leader transition.
type StateChange struct {
	Role   Role
	Term   Term
	Leader NodeID
}

// Ready is the work the driver must perform after stepping the core.
//
// The order is the contract and is not negotiable: persist HardState and
// Entries and synchronize them, then send Messages, then apply
// CommittedEntries. A driver that reorders these can emit a promise the
// member cannot keep after a crash, and the core cannot detect that it
// happened, which is why the simulator asserts the order on every batch.
type Ready struct {
	// HardState is non-nil only when term or vote changed.
	HardState *HardState

	// Entries must be appended to the log store before any message is sent.
	Entries []Entry

	// Messages may be sent only after the above are durable.
	Messages []Message

	// CommittedEntries are applied in index order after the messages are sent.
	CommittedEntries []Entry

	// StateChange is non-nil only when the role, term, or known leader changed.
	StateChange *StateChange
}

// IsEmpty reports whether a batch asks the driver to do anything at all.
func (r Ready) IsEmpty() bool {
	return r.HardState == nil && len(r.Entries) == 0 && len(r.Messages) == 0 &&
		len(r.CommittedEntries) == 0 && r.StateChange == nil
}
